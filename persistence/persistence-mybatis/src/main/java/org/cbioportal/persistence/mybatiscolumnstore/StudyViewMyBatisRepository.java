package org.cbioportal.persistence.mybatiscolumnstore;

import org.cbioportal.model.AlterationCountByGene;
import org.cbioportal.model.GenericAssayDataCount;
import org.cbioportal.model.Sample;
import org.cbioportal.persistence.StudyViewRepository;
import org.cbioportal.webparam.StudyViewFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class StudyViewMyBatisRepository implements StudyViewRepository {

    @Autowired
    private StudyViewMapper studyViewMapper;
    
    @Override
    public List<Sample> getFilteredSamplesFromColumnstore(StudyViewFilter studyViewFilter) {
        return studyViewMapper.getFilteredSamples(studyViewFilter);
    }
    
    @Override
    public List<AlterationCountByGene> getMutatedGenes(StudyViewFilter studyViewFilter) {
        return studyViewMapper.getMutatedGenes(studyViewFilter);
    }

    @Override
    public List<GenericAssayDataCount> getGenericAssayCountFromClickhouse(StudyViewFilter studyViewFilter, List<String> molecularProfileIds, List<String> stableIds) {
        return studyViewMapper.getGenericAssayCountFromClickhouse(studyViewFilter, molecularProfileIds, stableIds);
    }
}