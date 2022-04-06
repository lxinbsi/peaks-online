package com.bsi.peaks.server.handlers.specifications;

import com.bsi.peaks.data.graph.DbApplicationGraph;
import com.bsi.peaks.data.graph.LfqApplicationGraph;
import com.bsi.peaks.data.graph.RiqApplicationGraph;
import com.bsi.peaks.data.graph.SilacApplicationGraph;
import com.bsi.peaks.data.graph.SlApplicationGraph;
import com.bsi.peaks.io.writer.dto.DbDtoSources;
import com.bsi.peaks.io.writer.dto.DbDtoSourcesViaDbGraph;
import com.bsi.peaks.io.writer.dto.LfqDtoSources;
import com.bsi.peaks.io.writer.dto.LfqDtoSourcesViaLfqGraph;
import com.bsi.peaks.io.writer.dto.RiqDtoSources;
import com.bsi.peaks.io.writer.dto.RiqDtoSourcesViaRiqGraph;
import com.bsi.peaks.io.writer.dto.SilacDtoSources;
import com.bsi.peaks.io.writer.dto.SilacDtoSourcesViaSilacGraph;
import com.bsi.peaks.io.writer.dto.SlDtoSources;
import com.bsi.peaks.io.writer.dto.SlDtoSourcesViaSlGraph;

//For mocking unit tests
public class DtoSourceFactory {
    public DbDtoSources db(DbApplicationGraph applicationGraph) {
        return new DbDtoSourcesViaDbGraph(applicationGraph);
    }

    public SlDtoSources sl(SlApplicationGraph applicationGraph) {
        return new SlDtoSourcesViaSlGraph(applicationGraph);
    }

    public LfqDtoSources lfq(LfqApplicationGraph applicationGraph) {
        return new LfqDtoSourcesViaLfqGraph(applicationGraph);
    }

    public RiqDtoSources riq(RiqApplicationGraph applicationGraph) {
        return new RiqDtoSourcesViaRiqGraph(applicationGraph);
    }

    public SilacDtoSources silac(SilacApplicationGraph applicationGraph) {
        return new SilacDtoSourcesViaSilacGraph(applicationGraph);
    }
}
