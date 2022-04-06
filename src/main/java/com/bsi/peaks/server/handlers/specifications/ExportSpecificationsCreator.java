package com.bsi.peaks.server.handlers.specifications;

import akka.Done;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.bsi.peaks.data.graph.DbApplicationGraph;
import com.bsi.peaks.data.graph.SampleFractions;
import com.bsi.peaks.data.graph.SlApplicationGraph;
import com.bsi.peaks.data.graph.nodes.RefinedScanNode;
import com.bsi.peaks.data.graph.nodes.ScanNode;
import com.bsi.peaks.data.utils.StreamHelper;
import com.bsi.peaks.io.writer.csv.FeatureCsv;
import com.bsi.peaks.io.writer.dto.DbDtoSources;
import com.bsi.peaks.io.writer.dto.DbDtoSourcesViaDbGraph;
import com.bsi.peaks.io.writer.dto.SlDtoSources;
import com.bsi.peaks.io.writer.dto.SlDtoSourcesViaSlGraph;
import com.bsi.peaks.io.writer.mgf.Mgf;
import com.bsi.peaks.io.writer.xml.DBPepXml;
import com.bsi.peaks.io.writer.xml.DenovoPepXml;
import com.bsi.peaks.io.writer.xml.mzIdentML.MzIdentML;
import com.bsi.peaks.io.writer.xml.mzIdentML.MzdentML_1_1_0;
import com.bsi.peaks.io.writer.xml.mzxml.PepMzXml;
import com.bsi.peaks.model.query.IdentificationQuery;
import com.bsi.peaks.model.query.IdentificationType;
import com.bsi.peaks.model.query.ProteinViewFilter;
import com.bsi.peaks.model.query.SlQuery;
import com.bsi.peaks.reader.SourceTypeDetector;
import com.bsi.peaks.server.handlers.helper.ApplicationGraphFactory;
import com.bsi.peaks.server.handlers.helper.ExportHelpers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.sun.xml.txw2.output.IndentingXMLStreamWriter;
import org.apache.commons.io.output.CountingOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public interface ExportSpecificationsCreator {
    Logger LOG = LoggerFactory.getLogger(ExportSpecificationsCreator.class);
    ApplicationGraphFactory applicationGraphFactory();
    ObjectMapper om();

    default ExportSpecification<IdentificationQuery, DbApplicationGraph> proteinFastaSpecification() {
        return new ExportSpecification<>(
            "ProteinFasta",
            "text/plain",
            identificationQueryExtensionName("proteins.fasta"),
            this::proteinFastaProcess,
            applicationGraphFactory()::createFromIdentificationQuery
        );
    }

    default ExportSpecification<SlQuery, SlApplicationGraph> slProteinFastaSpecification() {
        return new ExportSpecification<>(
            "SlProteinFasta",
            "text/plain",
            (ignore, apiParams) -> {
                String searchType = apiParams.getOrDefault("search", "WorkFlow Unknown");
                return searchType + ".slProteins.fasta";
            },
            this::slProteinFastaProcess,
            applicationGraphFactory()::createFromSlQueryNoDecoy
        );
    }

    default CompletionStage<Done> proteinFastaProcess(
        String filename,
        String hostname,
        DbApplicationGraph applicationGraph,
        OutputStream outputStream,
        Materializer materializer,
        Map<String, String> request
    ) {
        DbDtoSources dtoSources = new DbDtoSourcesViaDbGraph(applicationGraph);
        try {
            ProteinViewFilter viewFilter = ExportHelpers.getViewFilter(om(), request, ProteinViewFilter.class);

            return dtoSources.dbSearchProteinSource()
                .filter(viewFilter::predicate)
                .fold(new HashSet<UUID>(), (set, protein) -> {
                    set.add(UUID.fromString(protein.getId()));
                    return set;
                })
                .flatMapConcat(set -> dtoSources.dbSearchProteinFastaSource(set))
                .runForeach(fastaSequence -> {
                    final String sequenceStr = fastaSequence.sequence();
                    byte[] header = fastaSequence.header().getBytes();
                    byte[] sequence = sequenceStr.getBytes();
                    outputStream.write(header);
                    outputStream.write('\n');
                    ExportHelpers.writeFastaSequence(outputStream, sequence, sequenceStr.length());
                }, materializer);
        } catch (Exception e) {
            CompletableFuture<Done> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }


    default CompletionStage<Done> slProteinFastaProcess(
        String filename,
        String hostname,
        SlApplicationGraph applicationGraph,
        OutputStream outputStream,
        Materializer materializer,
        Map<String, String> request
    ) {
        SlDtoSources dtoSources = new SlDtoSourcesViaSlGraph(applicationGraph);
        try {
            ProteinViewFilter viewFilter = ExportHelpers.getViewFilter(om(), request, ProteinViewFilter.class);

            return dtoSources.slProteinSource()
                .filter(viewFilter::predicate)
                .fold(new HashSet<UUID>(), (set, protein) -> {
                    set.add(UUID.fromString(protein.getId()));
                    return set;
                })
                .flatMapConcat(set -> dtoSources.slSearchProteinFastaSource(set))
                .runForeach(fastaSequence -> {
                    final String sequenceStr = fastaSequence.sequence();
                    byte[] header = fastaSequence.header().getBytes();
                    byte[] sequence = sequenceStr.getBytes();
                    outputStream.write(header);
                    outputStream.write('\n');
                    ExportHelpers.writeFastaSequence(outputStream, sequence, sequenceStr.length());
                }, materializer);
        } catch (Exception e) {
            CompletableFuture<Done> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    default ExportSpecification<IdentificationQuery, DbApplicationGraph> featureSpecification() {
        return new ExportSpecification<>(
            "Feature CSV",
            "text/csv",
            "feature.csv",
            this::featureProcess,
            applicationGraphFactory()::createFromIdentificationQuery
        );
    }

    default CompletionStage<Done> featureProcess(
        String filename,
        String hostname,
        DbApplicationGraph applicationGraph,
        OutputStream outputStream,
        Materializer materializer,
        Map<String, String> request
    ) {
        final FeatureCsv csv = new FeatureCsv(applicationGraph, FeatureCsv.DEFAULT_COLUMNS);
        return csv.lines().map(s -> s + "\n")
            .runWith(simpleSink(outputStream), materializer);
    }

    default ExportSpecification<IdentificationQuery, DbApplicationGraph> dbMzIdentMLSpecifications() {
        return new ExportSpecification<>(
            "MzIdentML",
            "text/xml",
            identificationQueryExtensionName("mzidentml.xml"),
            this::dbMzIdentMLProcess,
            applicationGraphFactory()::createFromIdentificationQuery
        );
    }

    default CompletionStage<Done> dbMzIdentMLProcess(
        String filename,
        String hostname,
        DbApplicationGraph applicationGraph,
        OutputStream outputStream,
        Materializer materializer,
        Map<String, String> request
    ) throws XMLStreamException {
        final MzIdentML pepXml = new MzdentML_1_1_0(applicationGraph, materializer, hostname);
        final OutputStream output = new BufferedOutputStream(outputStream);
        CountingOutputStream byteCountingStream = new CountingOutputStream(output);
        IndentingXMLStreamWriter xmlStreamWriter = new IndentingXMLStreamWriter(XMLOutputFactory.newInstance().createXMLStreamWriter(output, "UTF-8"));
        return pepXml.writeDocument(filename, xmlStreamWriter, byteCountingStream)
            .whenComplete((done, throwable) -> {
                flushOutput(output);
                if (throwable != null) {
                    LOG.error("Error during legacy export", throwable);
                }
            }).thenApply(ignore -> Done.getInstance());
    }

    default ExportSpecification<IdentificationQuery, DbApplicationGraph> dbPepXmlSpecifications() {
        return new ExportSpecification<>(
            "PepXML",
            "text/xml",
            identificationQueryExtensionName("pep.xml"),
            this::dbPepXmlProcess,
            applicationGraphFactory()::createFromIdentificationQuery
        );
    }

    default CompletionStage<Done> dbPepXmlProcess(
        String filename,
        String hostname,
        DbApplicationGraph applicationGraph,
        OutputStream outputStream,
        Materializer materializer,
        Map<String, String> request
    ) throws XMLStreamException {
        final DBPepXml pepXml = new DBPepXml(applicationGraph, materializer);
        final OutputStream output = new BufferedOutputStream(outputStream);
        CountingOutputStream byteCountingStream = new CountingOutputStream(output);
        return pepXml.writeDocument(filename, new IndentingXMLStreamWriter(XMLOutputFactory.newInstance().createXMLStreamWriter(outputStream, "UTF-8")), byteCountingStream)
            .whenComplete((done, throwable) -> {
                flushOutput(output);
                if (throwable != null) {
                    LOG.error("Error during legacy export", throwable);
                }
            }).thenApply(ignore -> Done.getInstance());
    }

    default void flushOutput(OutputStream output) {
        try {
            output.flush();
        } catch (IOException e) {
            LOG.error("Error flushing output stream", e);
        }
    }

    default CompletionStage<Done> denovoPepXmlProcess(
        String filename,
        String hostname,
        DbApplicationGraph applicationGraph,
        OutputStream outputStream,
        Materializer materializer
    ) throws XMLStreamException {
        final DenovoPepXml pepXml = new DenovoPepXml(applicationGraph, materializer);
        final OutputStream output = new BufferedOutputStream(outputStream);
        CountingOutputStream byteCountingStream = new CountingOutputStream(output);
        return pepXml.writeDocument(filename, new IndentingXMLStreamWriter(XMLOutputFactory.newInstance().createXMLStreamWriter(output, "UTF-8")), byteCountingStream)
            .whenComplete((done, throwable) -> {
                flushOutput(output);
                if (throwable != null) {
                    LOG.error("Error during legacy export", throwable);
                }
            }).thenApply(xmlStreamWriter -> Done.getInstance());
    }

    default Sink<String, CompletionStage<Done>> simpleSink(OutputStream outputStream) {
        return Sink.foreach(f -> { outputStream.write(f.getBytes()); });
    }

    default Function<IdentificationQuery, String> identificationQueryExtensionName(String extensionName) {
        return identificationQuery -> {
            if (identificationQuery.identificationType().equals(IdentificationType.NONE)) {
                return extensionName;
            } else {
                return IdentificationType.displayName(identificationQuery.identificationType()).toLowerCase() + "." + extensionName;
            }
        };
    }

    default ExportSpecification<IdentificationQuery, DbApplicationGraph> downloadMs2RawMGFSpecification() {
        return new ExportSpecification<>(
            "Ms2 Raw Mgf",
            "text/mgf",
            "raw.ms2.mgf",
            this::downloadMs2RawMGF,
            applicationGraphFactory()::createFromSampleOnly
        );
    }

    default CompletionStage<Done> downloadMs2RawMGF(
        String filename,
        String hostname,
        DbApplicationGraph applicationGraph,
        OutputStream outputStream,
        Materializer materializer,
        Map<String, String> request
    ) {
        final SampleFractions sample = Iterables.getOnlyElement(applicationGraph.sampleFractions());
        final UUID fractionId = Iterables.getOnlyElement(sample.fractionIds());
        return Source.fromCompletionStage(applicationGraph.fraction(fractionId).fraction())
            .via(StreamHelper.filterOptional())
            .flatMapConcat(fraction -> applicationGraph.fraction(fractionId).ms2ScanWithPeaksRaw().map(ScanNode::scan)
                .map(scan -> Mgf.toLocalString(fraction, scan, false))
            )
            .runWith(Sink.foreach(f -> outputStream.write(f.getBytes())), materializer);
    }

    default ExportSpecification<IdentificationQuery, DbApplicationGraph> downloadMs1Ms2RawMGFSpecification() {
        return new ExportSpecification<>(
            "Ms1Ms2 Raw Mgf",
            "text/mgf",
            "raw.ms1ms2.mgf",
            this::downloadMs1Ms2RawMGF,
            applicationGraphFactory()::createFromSampleOnly
        );
    }

    default CompletionStage<Done> downloadMs1Ms2RawMGF(
        String filename,
        String hostname,
        DbApplicationGraph applicationGraph,
        OutputStream outputStream,
        Materializer materializer,
        Map<String, String> request
    ) {
        final SampleFractions sample = Iterables.getOnlyElement(applicationGraph.sampleFractions());
        final UUID fractionId = Iterables.getOnlyElement(sample.fractionIds());
        return Source.fromCompletionStage(applicationGraph.fraction(fractionId).fraction())
            .via(StreamHelper.filterOptional())
            .flatMapConcat(fraction -> applicationGraph.fraction(fractionId).ms1Ms2WithPeaksUnrefined()
                .map(scan -> Mgf.toLocalString(fraction, scan, false))
            )
            .runWith(Sink.foreach(f -> outputStream.write(f.getBytes())), materializer);
    }

    default ExportSpecification<IdentificationQuery, DbApplicationGraph> downloadMs2RefinedMGFSpecification() {
        return new ExportSpecification<>(
            "Ms2 Refined Mgf",
            "text/mgf",
            "refined.mgf",
            this::downloadRefinedMGF,
            applicationGraphFactory()::createFromIdentificationQuery
        );
    }

    default ExportSpecification<IdentificationQuery, DbApplicationGraph> downloadPeaksProtoSpecification() {
        return new ExportSpecification<>(
            "Peaks Fraction Proto",
            "text/proto",
            // ppf is for timstof frames, pps is for other scan
            apiParameters -> apiParameters.containsKey("ionMobility") ?
                // have to separate these extensions be cause we need to know whether its tims tof data for loading
                SourceTypeDetector.PEAKS_RAW_FRAME_EXTENSION : SourceTypeDetector.PEAKS_RAW_SCAN_EXTENSION,
            this::downloadFractionProto,
            applicationGraphFactory()::createFromSampleOnly
        );
    }

    default CompletionStage<Done> downloadFractionProto(
            String filename,
            String hostname,
            DbApplicationGraph applicationGraph,
            OutputStream outputStream,
            Materializer materializer,
            Map<String, String> request
    ) {
        final SampleFractions sample = Iterables.getOnlyElement(applicationGraph.sampleFractions());
        final UUID fractionId = Iterables.getOnlyElement(sample.fractionIds());
        final float rtStart = request.containsKey("rtStart") ? Float.parseFloat(request.get("rtStart")) : 0f;
        final float rtEnd = request.containsKey("rtEnd") ? Float.parseFloat(request.get("rtEnd")) : Float.MAX_VALUE;
        return applicationGraph.fraction(fractionId).archiveSource(rtStart, rtEnd)
            .runWith(Sink.foreach(data -> data.writeDelimitedTo(outputStream)), materializer);
    }

    default CompletionStage<Done> downloadRefinedMGF(
        String filename,
        String hostname,
        DbApplicationGraph applicationGraph,
        OutputStream outputStream,
        Materializer materializer,
        Map<String, String> request
    ) {
        final SampleFractions sample = Iterables.getOnlyElement(applicationGraph.sampleFractions());
        final UUID fractionId = Iterables.getOnlyElement(sample.fractionIds());
        return Source.fromCompletionStage(applicationGraph.fraction(fractionId).fraction())
            .via(StreamHelper.filterOptional())
            .flatMapConcat(fraction ->applicationGraph.fraction(fractionId).refinedScanWithPeaks()
                .map(RefinedScanNode::scan)
                .map(scan -> Mgf.toLocalString(fraction, scan, true))
            )
            .runWith(Sink.foreach(f -> outputStream.write(f.getBytes())), materializer);
    }

    default ExportSpecification<IdentificationQuery, DbApplicationGraph> dbMzxmlSpecifications() {
        return new ExportSpecification<>(
            "mzxml",
            "text/xml",
            "mzxml",
            this::downloadMzxml,
            applicationGraphFactory()::createFromIdentificationQuery
        );
    }

    default CompletionStage<Done> downloadMzxml(
        String filename,
        String hostname,
        DbApplicationGraph applicationGraph,
        OutputStream outputStream,
        Materializer materializer,
        Map<String, String> request
    ) throws XMLStreamException {
        final SampleFractions sample = Iterables.getOnlyElement(applicationGraph.sampleFractions());
        final UUID fractionId = Iterables.getOnlyElement(sample.fractionIds());
        final PepMzXml mzxml = new PepMzXml(fractionId, applicationGraph, materializer);
        final OutputStream output = new BufferedOutputStream(outputStream);
        CountingOutputStream byteCountingStream = new CountingOutputStream(output);
        IndentingXMLStreamWriter xmlStreamWriter = new IndentingXMLStreamWriter(XMLOutputFactory.newInstance().createXMLStreamWriter(byteCountingStream, "UTF-8"));
        return mzxml.writeDocument(filename, xmlStreamWriter, byteCountingStream)
            .whenComplete((done, throwable) -> {
                flushOutput(output);
                if (throwable != null) {
                    LOG.error("Error during legacy export", throwable);
                }
            }).thenApply(ignore -> Done.getInstance());
    }


}
