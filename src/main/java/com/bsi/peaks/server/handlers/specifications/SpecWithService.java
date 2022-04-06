package com.bsi.peaks.server.handlers.specifications;

import akka.Done;
import com.bsi.peaks.model.system.User;

import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public interface SpecWithService extends Spec{
    CompletionStage<Done> generateTxt(OutputStream outputStream, Map<String, String> request, User user);

    @Override
    default Spec.SpecType getType() {
        return Spec.SpecType.Services;
    }
}
