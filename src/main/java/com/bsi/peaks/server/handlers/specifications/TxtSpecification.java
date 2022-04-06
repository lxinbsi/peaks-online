package com.bsi.peaks.server.handlers.specifications;

import akka.Done;
import com.bsi.peaks.io.writer.txt.Txt;
import com.bsi.peaks.model.system.User;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

public class TxtSpecification implements SpecWithService {
    private final String fileName;
    private final BiFunction<Map<String, String>, User, CompletionStage<Txt>> txtGenerator;

    public TxtSpecification(String fileName, BiFunction<Map<String, String>, User, CompletionStage<Txt>> txtGenerator) {
        this.fileName = fileName;
        this.txtGenerator = txtGenerator;
    }

    public String getFileName() {
        return fileName;
    }

    @Override
    public CompletionStage<Done> generateTxt(OutputStream outputStream, Map<String, String> request, User user) {
        return txtGenerator.apply(request, user)
            .thenApply(txt -> {
                try {
                    txt.output(outputStream);
                } catch (Exception e) {
                    Throwables.throwIfUnchecked(e);
                    throw new CompletionException(e);
                }
                return Done.getInstance();
            });
    }
}
