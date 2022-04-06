package com.bsi.peaks.server.es.spectrallibrary;

import akka.NotUsed;
import akka.japi.Pair;
import akka.japi.Procedure;
import akka.japi.function.Function;
import akka.japi.tuple.Tuple4;
import akka.stream.Materializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.bsi.peaks.common.floats.FloatConsumer;
import com.bsi.peaks.data.storage.application.ApplicationStorageFactory;
import com.bsi.peaks.data.storage.application.SpectralLibraryRepository;
import com.bsi.peaks.data.utils.StreamHelper;
import com.bsi.peaks.event.spectralLibrary.SLProcessTaskIds;
import com.bsi.peaks.internal.task.SlSearchParameters;
import com.bsi.peaks.io.writer.csv.SpectralLibrary.SpectralLibraryColumns;
import com.bsi.peaks.model.ModelConversion;
import com.bsi.peaks.model.filter.PeptideFilter;
import com.bsi.peaks.model.filter.PsmFilter;
import com.bsi.peaks.model.filter.SLPsmFilter;
import com.bsi.peaks.model.filter.SlPeptideFilter;
import com.bsi.peaks.model.parameters.DBSearchParameters;
import com.bsi.peaks.model.proto.AcquisitionMethod;
import com.bsi.peaks.model.proto.Modification;
import com.bsi.peaks.model.sl.SLSpectrum;
import com.bsi.peaks.model.sl.SLSpectrumBuilder;
import com.bsi.peaks.service.common.ServiceKiller;
import com.bsi.peaks.service.services.libgenerator.*;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.bsi.peaks.io.writer.export.ExportFunction.GENERIC_4DECIMAL_FORMAT;
import static com.bsi.peaks.io.writer.export.ExportFunction.GENERIC_5DECIMAL_FORMAT;
import static com.bsi.peaks.model.ModelConversion.uuidsFrom;

public class ProcessSpectralLibrary {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessSpectralLibrary.class);
    private static final float PRE_PROCESS_PERCENTAGE = 0.20f;
    private static final float MAIN_PROCESS_PERCENTAGE = 1f - PRE_PROCESS_PERCENTAGE;
    private static final float PSM_FDR = 0.25f;
    private static ObjectMapper OM = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .registerModule(new ProtobufModule())
        .registerModule(new Jdk8Module());

    private final ApplicationStorageFactory storageFactory;
    private final BuildAdjustPsmRtFunction buildAdjustPsmRtFunction;
    private final BuildLibraryGenerator buildLibraryGenerator;
    private int parallelism;
    private int rateLimitProgressEmitsPerSecond;
    private java.util.function.Function<List<Long>, DecoyGenerator> decoyGeneratorConstructor;

    @Inject
    public ProcessSpectralLibrary(
        ApplicationStorageFactory storageFactory,
        BuildAdjustPsmRtFunction buildAdjustPsmRtFunction,
        BuildLibraryGenerator buildLibraryGenerator
    ) {
        this(
            storageFactory,
            buildAdjustPsmRtFunction,
            buildLibraryGenerator,
            DecoyGenerator::new,
            storageFactory.getParallelism()
        );
    }

    public ProcessSpectralLibrary(
        ApplicationStorageFactory storageFactory,
        BuildAdjustPsmRtFunction buildAdjustPsmRtFunction,
        BuildLibraryGenerator buildLibraryGenerator,
        java.util.function.Function<List<Long>, DecoyGenerator> decoyGeneratorConstructor,
        int parallelism
    ) {
        this.storageFactory = storageFactory;
        this.buildAdjustPsmRtFunction = buildAdjustPsmRtFunction;
        this.buildLibraryGenerator = buildLibraryGenerator;
        this.decoyGeneratorConstructor = decoyGeneratorConstructor;
        this.parallelism = parallelism;
    }

    private List<FractionToProcess> fractionToProcessSource(SLProcessTaskIds taskIds) throws IOException {
        List<FractionToProcess> list = new ArrayList<>();
        Iterator<UUID> fractionIds = uuidsFrom(taskIds.getFractionIds()).iterator();
        Iterator<UUID> dataLoadingTaskIds = uuidsFrom(taskIds.getDataLoadingTaskIds()).iterator();
        Iterator<UUID> dbSearchTaskIds = uuidsFrom(taskIds.getDbSearchTaskIds()).iterator();
        if (taskIds.getAcquisitionMethod().equals(AcquisitionMethod.DIA)) {
            UUID firstFractionId = uuidsFrom(taskIds.getFractionIds()).iterator().next();
            boolean isTims = storageFactory.getStorage(taskIds.getKeyspace()).getFractionRepository()
                    .getAttributesById(firstFractionId)
                    .thenApply(f -> f.map(p -> p.frameIonMobilities().size() > 0).orElse(false))
                    .toCompletableFuture()
                    .join();
            final SlSearchParameters slSearchParameters = taskIds.getSlSearchParameters();
            final SLPsmFilter slPsmFilter = SLPsmFilter.fromProto(taskIds.getSlPsmFilter());
            final SlPeptideFilter slPeptideFilter = SlPeptideFilter.fromProtoNoFdrConversion(taskIds.getSlPeptideFilter());
            final List<Modification> modifications = taskIds.getModificationsList();
            while (fractionIds.hasNext() && dataLoadingTaskIds.hasNext() && dbSearchTaskIds.hasNext()) {
                list.add(new DiaFractionToProcess(taskIds.getKeyspace(), fractionIds.next(),
                        dataLoadingTaskIds.next(), dbSearchTaskIds.next(), slSearchParameters, slPsmFilter, slPeptideFilter, modifications, isTims));
            }
        } else {
            final DBSearchParameters dbSearchParameters = OM.readValue(taskIds.getDbSearchParametersJSON(), DBSearchParameters.class);
            final PsmFilter psmFilter = PsmFilter.fromDto(taskIds.getPsmFilter());
            final PeptideFilter peptideFilter = PeptideFilter.fromProtoNoFdrConversion(taskIds.getPeptideFilter());
            while (fractionIds.hasNext() && dataLoadingTaskIds.hasNext() && dbSearchTaskIds.hasNext()) {
                list.add(new DdaFractionToProcess(taskIds.getKeyspace(), fractionIds.next(),
                        dataLoadingTaskIds.next(), dbSearchTaskIds.next(), dbSearchParameters, psmFilter, peptideFilter));
            }
        }
        return list;
    }

    public CompletionStage<Integer> process(
        UUID spectralLibraryId,
        SLProcessTaskIds taskIds,
        Materializer materializer,
        FloatConsumer emitProgressInput,
        ServiceKiller serviceKiller,
        boolean useRtAsIRt
    ) throws IOException {
        RateLimiter rateLimiter = RateLimiter.create(5);
        Procedure<Float> emitProgress = pct -> {
            if (rateLimiter.tryAcquire()) emitProgressInput.accept(pct);
        };

        AtomicInteger rtFunctionProgress = new AtomicInteger();
        List<FractionToProcess> fractionToProcesses = fractionToProcessSource(taskIds);

        float sourcePercentage = PRE_PROCESS_PERCENTAGE / fractionToProcesses.size();
        CompletionStage<List<FractionToProcessWithRtFunction>> futureRtFunctions = Source.from(fractionToProcesses)
            .via(serviceKiller.flow())
            // Remove duplicate fractions.
            .statefulMapConcat(() -> {
                Set<UUID> fractionIds = new HashSet<>();
                return (FractionToProcess process) -> {
                    if (fractionIds.add(process.fractionId())) {
                        emitProgress.apply(rtFunctionProgress.incrementAndGet() * sourcePercentage);
                        return Collections.singletonList(process);
                    } else {
                        return Collections.emptyList();
                    }
                };
            })
            .via(buildRtFunction())
            .runFold(ImmutableList.<FractionToProcessWithRtFunction>builder(), (l, e) -> {
                emitProgress.apply(rtFunctionProgress.incrementAndGet() * sourcePercentage);
                return l.add(e);
            }, materializer)
            .thenApply(ImmutableList.Builder::build);

        final SpectralLibraryRepository spectralLibraryRepository = storageFactory.getSystemStorage().getSpectralLibraryRepository();

        CompletionStage<List<Long>> rtListAfterWritingTarget = futureRtFunctions
            .thenCompose(p -> {
                List<FractionToProcessWithRtFunction> rtFunctions = p;
                long totalPsm = rtFunctions.stream().mapToLong(FractionToProcessWithRtFunction::psmCount).sum();
                return sourceGroupedByPeptideAndCharge(rtFunctions, useRtAsIRt)
                    .via(serviceKiller.flow())
                    .async()
                    .statefulMapConcat(() -> new Function<List<AdjustPsmRetentionTime.AdjustedRTPsmWithPeaks>, Iterable<SLSpectrum>>() {

                        long psmCount = 0;
                        int index = 0;

                        @Override
                        public Iterable<SLSpectrum> apply(List<AdjustPsmRetentionTime.AdjustedRTPsmWithPeaks> adjustedRTPsmWithPeaks) throws Exception {
                            long nextCnt = psmCount += adjustedRTPsmWithPeaks.size();
                            emitProgress.apply(Math.max(nextCnt / totalPsm * MAIN_PROCESS_PERCENTAGE + PRE_PROCESS_PERCENTAGE, 1f)); //Bug to be fixed

                            return buildLibraryGenerator.build(adjustedRTPsmWithPeaks)
                                .map(slSpectrum -> slSpectrum.withIndex(index++))
                                .map(Collections::singletonList)
                                .orElse(Collections.emptyList());
                        }
                    })
                    .map(ProcessSpectralLibrary::roundToExportValues)
                    .alsoToMat(spectralLibraryRepository.sinkTarget(spectralLibraryId), Keep.right())
                    .toMat(Sink.fold(ImmutableList.<Long>builder(), (list, spectrum) -> list.add(spectrum.retentionTimeMs())), (futureDone, listBuilder) -> futureDone.thenCompose(done -> listBuilder.<List<Long>>thenApply(ImmutableList.Builder::build)))
                    .run(materializer);
        });

        return rtListAfterWritingTarget
            .thenCompose(spectralLibraryRts -> {
                // TODO DecoyGenerator mutates the constructor input ... Better to remove Lists.newArrayList than allow this mutation to happen.
                DecoyGenerator decoyGenerator = decoyGeneratorConstructor.apply(Lists.newArrayList(spectralLibraryRts));
                return spectralLibraryRepository.sourceOrderedOnlyTarget(spectralLibraryId)
                    .mapConcat(decoyGenerator::generateDecoys)
                    .runWith(spectralLibraryRepository.sinkDecoy(spectralLibraryId), materializer)
                    .thenApply(done -> spectralLibraryRts.size());
            });
    }

    private static SLSpectrum roundToExportValues(SLSpectrum x) {
        float[] mzs = x.mzs();
        for (int i = 0; i < mzs.length; i++) {
            mzs[i] = Float.parseFloat(GENERIC_5DECIMAL_FORMAT.format(mzs[i]));
        }
        float[] intensities = x.intensities();
        for (int i = 0; i < intensities.length; i++) {
            intensities[i] = Float.parseFloat(GENERIC_4DECIMAL_FORMAT.format(intensities[i]));
        }
        return new SLSpectrumBuilder()
            .from(x)
            .precursorMz(Float.parseFloat(GENERIC_5DECIMAL_FORMAT.format(x.precursorMz())))
            .retentionTimeMs((long)(Double.parseDouble(SpectralLibraryColumns.RETENTION_TIME.exportCell(x)) * 1_000))
            .mzs(mzs)
            .intensities(intensities)
            .build();
    }

    private Source<List<AdjustPsmRetentionTime.AdjustedRTPsmWithPeaks>, NotUsed> sourceGroupedByPeptideAndCharge(
        List<FractionToProcessWithRtFunction> list,
        boolean useRtAsIRt
    ) {
        List<Source<FractionToProcessWithRtFunction.PsmForScanQuerying, NotUsed>> s = new ArrayList<>(list.size());
        for (FractionToProcessWithRtFunction process : list) {
            s.add(process.createFilteredPsmRtAdjustableSource(storageFactory));
        }

        AdjustPsmRetentionTimeFunction[] adjustPsmRts = list.stream()
            .map(FractionToProcessWithRtFunction::adjustPsmRetentionTimeFunction)
            .toArray(AdjustPsmRetentionTimeFunction[]::new);

        AdjustPsmRetentionTime adjustPsmRetentionTime = new AdjustPsmRetentionTime(adjustPsmRts);

        return StreamHelper
            .orderedMerge(s, Comparator.comparing(FractionToProcessWithRtFunction.PsmForScanQuerying::peptide))
            .async()
            .mapAsync(parallelism, t -> {
                return t.queryScan()
                    .thenApply(apexScanWithPeaks -> Tuple4.create(t.peptide(), t.massParameter(), t.psm(), apexScanWithPeaks));
            })
            .via(StreamHelper.splitAndGroupBy(Tuple4::t1))
            .mapConcat(p -> p.second().stream().collect(Collectors
                .groupingBy(
                    t -> new Pair<>(t.t3().precursorCharge(), t.t3().faimsCv()),
                    Collectors.mapping(t -> adjustPsmRetentionTime.adjust(t.t3(), t.t4(), t.t2(), useRtAsIRt), Collectors.toList()
                    ))).values())
            .alsoTo(Sink.fold(0, (cnt, element) -> {
                if (cnt % 10000 == 0) {
                    LOG.info("Emitting peptide charge group... " + cnt);
                }
                return ++cnt;
            }));
    }

    private Flow<FractionToProcess, FractionToProcessWithRtFunction, NotUsed> buildRtFunction() {
        return Flow.<FractionToProcess>create()
            .flatMapMerge(parallelism, (FractionToProcess process) -> process.createFractionToProcessWithRtFunctionSource(storageFactory)
                .map(retentionTimes -> process.withFractionToProcessWithRtFunction(
                    retentionTimes.size(),
                    buildAdjustPsmRtFunction.build(process.fractionId(), retentionTimes)
                )));
    }
}
