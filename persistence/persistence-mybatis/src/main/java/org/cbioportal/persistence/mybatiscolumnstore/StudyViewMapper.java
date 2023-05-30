package org.cbioportal.persistence.mybatiscolumnstore;

import org.cbioportal.model.AlterationCountByGene;
import org.cbioportal.model.GenericAssayDataCount;
import org.cbioportal.model.Sample;
import org.cbioportal.webparam.StudyViewFilter;

import java.util.List;

public interface StudyViewMapper {
    List<Sample> getFilteredSamples(StudyViewFilter studyViewFilter);
    List<AlterationCountByGene> getMutatedGenes(StudyViewFilter studyViewFilter);
    List<GenericAssayDataCount>  getGenericAssayCountFromClickhouse(StudyViewFilter studyViewFilter, List<String> molecularProfileIds, List<String> stableIds);
}
