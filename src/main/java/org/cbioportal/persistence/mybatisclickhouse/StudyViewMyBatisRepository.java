package org.cbioportal.persistence.mybatisclickhouse;
import org.cbioportal.model.AlterationCountByGene;
import org.cbioportal.model.ClinicalAttribute;
import org.cbioportal.model.CaseListDataCount;
import org.cbioportal.model.ClinicalData;
import org.cbioportal.model.ClinicalDataCount;
import org.cbioportal.model.ClinicalEventTypeCount;
import org.cbioportal.model.GenePanelToGene;
import org.cbioportal.model.GenomicDataCount;
import org.cbioportal.model.CopyNumberCountByGene;
import org.cbioportal.model.MolecularProfile;
import org.cbioportal.model.Sample;
import org.cbioportal.persistence.StudyViewRepository;
import org.cbioportal.persistence.enums.ClinicalAttributeDataSource;
import org.cbioportal.persistence.helper.AlterationFilterHelper;
import org.cbioportal.web.parameter.CategorizedClinicalDataCountFilter;
import org.cbioportal.web.parameter.ClinicalDataType;
import org.cbioportal.web.parameter.StudyViewFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Repository
public class StudyViewMyBatisRepository implements StudyViewRepository {

    private Map<ClinicalAttributeDataSource, List<ClinicalAttribute>> clinicalAttributesMap = new HashMap<>();
    private Map<ClinicalAttributeDataSource, List<MolecularProfile>> genericAssayProfilesMap = new HashMap<>();


    private static final List<String> FILTERED_CLINICAL_ATTR_VALUES = Collections.emptyList();
    private final StudyViewMapper mapper;
   
    @Autowired
    public StudyViewMyBatisRepository(StudyViewMapper mapper) {
        this.mapper = mapper;    
    }
    @Override
    public List<Sample> getFilteredSamples(StudyViewFilter studyViewFilter) {
        CategorizedClinicalDataCountFilter categorizedClinicalDataCountFilter = extractDataCountFilters(studyViewFilter);
        return mapper.getFilteredSamples(studyViewFilter, categorizedClinicalDataCountFilter, shouldApplyPatientIdFilters(studyViewFilter,categorizedClinicalDataCountFilter));
    }
    
    @Override
    public List<AlterationCountByGene> getMutatedGenes(StudyViewFilter studyViewFilter) {
        CategorizedClinicalDataCountFilter categorizedClinicalDataCountFilter = extractDataCountFilters(studyViewFilter);
        return mapper.getMutatedGenes(studyViewFilter, categorizedClinicalDataCountFilter,
            shouldApplyPatientIdFilters(studyViewFilter,categorizedClinicalDataCountFilter),
            AlterationFilterHelper.build(studyViewFilter.getAlterationFilter()));
    }

    @Override
    public List<CopyNumberCountByGene> getCnaGenes(StudyViewFilter studyViewFilter) {
        CategorizedClinicalDataCountFilter categorizedClinicalDataCountFilter = extractDataCountFilters(studyViewFilter);
        return mapper.getCnaGenes(studyViewFilter, categorizedClinicalDataCountFilter,
            shouldApplyPatientIdFilters(studyViewFilter,categorizedClinicalDataCountFilter),
            AlterationFilterHelper.build(studyViewFilter.getAlterationFilter()));
    }

    @Override
    public List<AlterationCountByGene> getStructuralVariantGenes(StudyViewFilter studyViewFilter) {
        CategorizedClinicalDataCountFilter categorizedClinicalDataCountFilter = extractDataCountFilters(studyViewFilter);
        return mapper.getStructuralVariantGenes(studyViewFilter, categorizedClinicalDataCountFilter,
            shouldApplyPatientIdFilters(studyViewFilter,categorizedClinicalDataCountFilter),
            AlterationFilterHelper.build(studyViewFilter.getAlterationFilter()));
    }

    @Override
    public List<ClinicalDataCount> getClinicalDataCounts(StudyViewFilter studyViewFilter, List<String> filteredAttributes) {
        CategorizedClinicalDataCountFilter categorizedClinicalDataCountFilter = extractDataCountFilters(studyViewFilter);
        return mapper.getClinicalDataCounts(studyViewFilter, categorizedClinicalDataCountFilter,
            shouldApplyPatientIdFilters(studyViewFilter,categorizedClinicalDataCountFilter),
            filteredAttributes, FILTERED_CLINICAL_ATTR_VALUES );
    }

    @Override
    public List<GenomicDataCount> getGenomicDataCounts(StudyViewFilter studyViewFilter) {
        CategorizedClinicalDataCountFilter categorizedClinicalDataCountFilter = extractDataCountFilters(studyViewFilter);
        return mapper.getGenomicDataCounts(studyViewFilter, categorizedClinicalDataCountFilter,
            shouldApplyPatientIdFilters(studyViewFilter,categorizedClinicalDataCountFilter));
    }
    
    @Override
    public List<ClinicalAttribute> getClinicalAttributes() {
        return mapper.getClinicalAttributes();
    }

    @Override
    public List<MolecularProfile> getGenericAssayProfiles() {
        return mapper.getGenericAssayProfiles();
    }

    @Override
    public Map<String, ClinicalDataType> getClinicalAttributeDatatypeMap() {
        if (clinicalAttributesMap.isEmpty()) {
            buildClinicalAttributeNameMap();
        }
        
        Map<String, ClinicalDataType> attributeDatatypeMap = new HashMap<>();

        clinicalAttributesMap
            .get(ClinicalAttributeDataSource.SAMPLE)
            .forEach(attribute -> attributeDatatypeMap.put(attribute.getAttrId(), ClinicalDataType.SAMPLE));

        clinicalAttributesMap
            .get(ClinicalAttributeDataSource.PATIENT)
            .forEach(attribute -> attributeDatatypeMap.put(attribute.getAttrId(), ClinicalDataType.PATIENT));
        
        return attributeDatatypeMap;
    }

    @Override
    public List<CaseListDataCount> getCaseListDataCounts(StudyViewFilter studyViewFilter) {
        CategorizedClinicalDataCountFilter categorizedClinicalDataCountFilter = extractDataCountFilters(studyViewFilter);
        return mapper.getCaseListDataCounts(studyViewFilter, categorizedClinicalDataCountFilter, shouldApplyPatientIdFilters(studyViewFilter,categorizedClinicalDataCountFilter));
    }


    private boolean shouldApplyPatientIdFilters(StudyViewFilter studyViewFilter, CategorizedClinicalDataCountFilter categorizedClinicalDataCountFilter) {
        return studyViewFilter.getClinicalEventFilters() != null && !studyViewFilter.getClinicalEventFilters().isEmpty() 
        || categorizedClinicalDataCountFilter.getPatientCategoricalClinicalDataFilters() != null && !categorizedClinicalDataCountFilter.getPatientCategoricalClinicalDataFilters().isEmpty()
            || categorizedClinicalDataCountFilter.getPatientNumericalClinicalDataFilters() != null && !categorizedClinicalDataCountFilter.getPatientNumericalClinicalDataFilters().isEmpty();
    }

    @Override
    public List<ClinicalData> getSampleClinicalData(StudyViewFilter studyViewFilter, List<String> attributeIds) {
        CategorizedClinicalDataCountFilter categorizedClinicalDataCountFilter = extractDataCountFilters(studyViewFilter);
        return mapper.getSampleClinicalDataFromStudyViewFilter(studyViewFilter, categorizedClinicalDataCountFilter, shouldApplyPatientIdFilters(studyViewFilter,categorizedClinicalDataCountFilter), attributeIds);
    }
    
    @Override
    public List<ClinicalData> getPatientClinicalData(StudyViewFilter studyViewFilter, List<String> attributeIds) {
        CategorizedClinicalDataCountFilter categorizedClinicalDataCountFilter = extractDataCountFilters(studyViewFilter);
        return mapper.getPatientClinicalDataFromStudyViewFilter(studyViewFilter, categorizedClinicalDataCountFilter, shouldApplyPatientIdFilters(studyViewFilter,categorizedClinicalDataCountFilter), attributeIds);
    }
    
    @Override
    public Map<String, AlterationCountByGene> getTotalProfiledCounts(StudyViewFilter studyViewFilter, String alterationType) {
        CategorizedClinicalDataCountFilter categorizedClinicalDataCountFilter = extractDataCountFilters(studyViewFilter);
        return mapper.getTotalProfiledCounts(studyViewFilter, categorizedClinicalDataCountFilter,
            shouldApplyPatientIdFilters(studyViewFilter,categorizedClinicalDataCountFilter), alterationType);
    }

    @Override
    public int getFilteredSamplesCount(StudyViewFilter studyViewFilter) {
        CategorizedClinicalDataCountFilter categorizedClinicalDataCountFilter = extractDataCountFilters(studyViewFilter);
        return mapper.getFilteredSamplesCount(studyViewFilter, categorizedClinicalDataCountFilter,
            shouldApplyPatientIdFilters(studyViewFilter,categorizedClinicalDataCountFilter));
    }

    @Override
    public Map<String, Set<String>> getMatchingGenePanelIds(StudyViewFilter studyViewFilter, String alterationType) {
        CategorizedClinicalDataCountFilter categorizedClinicalDataCountFilter = extractDataCountFilters(studyViewFilter);
        return mapper.getMatchingGenePanelIds(studyViewFilter, categorizedClinicalDataCountFilter,
            shouldApplyPatientIdFilters(studyViewFilter,categorizedClinicalDataCountFilter), alterationType)
            .stream()
            .collect(Collectors.groupingBy(GenePanelToGene::getHugoGeneSymbol,
                Collectors.mapping(GenePanelToGene::getGenePanelId, Collectors.toSet())));
    }

    @Override
    public int getTotalProfiledCountsByAlterationType(StudyViewFilter studyViewFilter, String alterationType) {
        CategorizedClinicalDataCountFilter categorizedClinicalDataCountFilter = extractDataCountFilters(studyViewFilter);
       return mapper.getTotalProfiledCountByAlterationType(studyViewFilter, categorizedClinicalDataCountFilter,
           shouldApplyPatientIdFilters(studyViewFilter,categorizedClinicalDataCountFilter), alterationType); 
    }

    @Override
    public List<ClinicalEventTypeCount> getClinicalEventTypeCounts(StudyViewFilter studyViewFilter) {
        CategorizedClinicalDataCountFilter categorizedClinicalDataCountFilter = extractDataCountFilters(studyViewFilter);
        return mapper.getClinicalEventTypeCounts(studyViewFilter, categorizedClinicalDataCountFilter,
            shouldApplyPatientIdFilters(studyViewFilter,categorizedClinicalDataCountFilter));
    }

    // TODO: update parameter name
    @Override
    public List<ClinicalDataCount> getGenomicDataBinCounts(StudyViewFilter studyViewFilter, List<String> filteredAttributes) {
        CategorizedClinicalDataCountFilter categorizedClinicalDataCountFilter = extractDataCountFilters(studyViewFilter);
        return mapper.getGenomicDataBinCounts(studyViewFilter, categorizedClinicalDataCountFilter,
            // TODO: This might need to update with patient level information
            // setting false to indicate this is sample level data
            false,
            filteredAttributes, Collections.emptyList() );
    }

    // TODO: update parameter name
    @Override
    public List<ClinicalDataCount> getGenericAssayDataBinCounts(StudyViewFilter studyViewFilter, List<String> filteredAttributes) {
        CategorizedClinicalDataCountFilter categorizedClinicalDataCountFilter = extractDataCountFilters(studyViewFilter);
        return mapper.getGenericAssayDataBinCounts(studyViewFilter, categorizedClinicalDataCountFilter,
            // TODO: This might need to update with patient level information
            // setting false to indicate this is sample level data
            false,
            filteredAttributes, Collections.emptyList() );
    }

    private void buildClinicalAttributeNameMap() {
        clinicalAttributesMap = this.getClinicalAttributes()
            .stream()
            .collect(Collectors.groupingBy(ca -> ca.getPatientAttribute() ? ClinicalAttributeDataSource.PATIENT : ClinicalAttributeDataSource.SAMPLE));
    }

    private void buildGenericAssayProfilesMap() {
        genericAssayProfilesMap = this.getGenericAssayProfiles()
            .stream()
            .collect(Collectors.groupingBy(ca -> ca.getPatientLevel() ? ClinicalAttributeDataSource.PATIENT : ClinicalAttributeDataSource.SAMPLE));
    }
    
    private CategorizedClinicalDataCountFilter extractDataCountFilters(final StudyViewFilter studyViewFilter) {
        if (clinicalAttributesMap.isEmpty()) {
            buildClinicalAttributeNameMap();
        }

        if (genericAssayProfilesMap.isEmpty()) {
            buildGenericAssayProfilesMap();
        }

        if (studyViewFilter.getClinicalDataFilters() == null && studyViewFilter.getGenomicDataFilters() == null && studyViewFilter.getGenericAssayDataFilters() == null) {
            return CategorizedClinicalDataCountFilter.getBuilder().build();
        }
        
        List<String> patientCategoricalAttributes = clinicalAttributesMap.get(ClinicalAttributeDataSource.PATIENT)
            .stream().filter(ca -> ca.getDatatype().equals("STRING"))
            .map(ClinicalAttribute::getAttrId)
            .toList();

        List<String> patientNumericalAttributes = clinicalAttributesMap.get(ClinicalAttributeDataSource.PATIENT)
            .stream().filter(ca -> ca.getDatatype().equals("NUMBER"))
            .map(ClinicalAttribute::getAttrId)
            .toList();

        List<String> sampleCategoricalAttributes = clinicalAttributesMap.get(ClinicalAttributeDataSource.SAMPLE)
            .stream().filter(ca -> ca.getDatatype().equals("STRING"))
            .map(ClinicalAttribute::getAttrId)
            .toList();

        List<String> sampleNumericalAttributes = clinicalAttributesMap.get(ClinicalAttributeDataSource.SAMPLE)
            .stream().filter(ca -> ca.getDatatype().equals("NUMBER"))
            .map(ClinicalAttribute::getAttrId)
            .toList();

        CategorizedClinicalDataCountFilter.Builder builder = CategorizedClinicalDataCountFilter.getBuilder();
        if (studyViewFilter.getClinicalDataFilters() != null) {
            builder.setPatientCategoricalClinicalDataFilters(studyViewFilter.getClinicalDataFilters()
                    .stream().filter(clinicalDataFilter -> patientCategoricalAttributes.contains(clinicalDataFilter.getAttributeId()))
                    .collect(Collectors.toList()))
                .setPatientNumericalClinicalDataFilters(studyViewFilter.getClinicalDataFilters().stream()
                    .filter(clinicalDataFilter -> patientNumericalAttributes.contains(clinicalDataFilter.getAttributeId()))
                    .collect(Collectors.toList()))
                .setSampleCategoricalClinicalDataFilters(studyViewFilter.getClinicalDataFilters().stream()
                    .filter(clinicalDataFilter -> sampleCategoricalAttributes.contains(clinicalDataFilter.getAttributeId()))
                    .collect(Collectors.toList()))
                .setSampleNumericalClinicalDataFilters(studyViewFilter.getClinicalDataFilters().stream()
                    .filter(clinicalDataFilter -> sampleNumericalAttributes.contains(clinicalDataFilter.getAttributeId()))
                    .collect(Collectors.toList()));
        }
        if (studyViewFilter.getGenomicDataFilters() != null) {
            //            .setPatientCategoricalGenomicDataFilters(studyViewFilter.getClinicalDataFilters()
            //                .stream().filter(genomicDataFilter -> patientCategoricalAttributes.contains(genomicDataFilter.getAttributeId()))
            //                .collect(Collectors.toList()))
            //            .setPatientNumericalGenomicDataFilters(studyViewFilter.getClinicalDataFilters().stream()
            //                .filter(genomicDataFilter -> patientNumericalAttributes.contains(genomicDataFilter.getAttributeId()))
            //                .collect(Collectors.toList()))
            //            .setSampleCategoricalGenomicDataFilters(studyViewFilter.getClinicalDataFilters().stream()
            //                .filter(genomicDataFilter -> sampleCategoricalAttributes.contains(genomicDataFilter.getAttributeId()))
            //                .collect(Collectors.toList()))
            builder.setSampleNumericalGenomicDataFilters(studyViewFilter.getGenomicDataFilters().stream()
                .filter(genomicDataFilter -> genomicDataFilter.getProfileType() != "cna" && genomicDataFilter.getProfileType() != "gistic")
                .collect(Collectors.toList()));
        }
        if (studyViewFilter.getGenericAssayDataFilters() != null) {
            //            .setPatientCategoricalGenericAssayDataFilters(studyViewFilter.getClinicalDataFilters()
            //                .stream().filter(genericAssayDataFilter -> patientCategoricalAttributes.contains(genericAssayDataFilter.getAttributeId()))
            //                .collect(Collectors.toList()))
            //            .setPatientNumericalGenericAssayDataFilters(studyViewFilter.getClinicalDataFilters().stream()
            //                .filter(genericAssayDataFilter -> patientNumericalAttributes.contains(genericAssayDataFilter.getAttributeId()))
            //                .collect(Collectors.toList()))
            //            .setSampleCategoricalGenericAssayDataFilters(studyViewFilter.getClinicalDataFilters().stream()
            //                .filter(genericAssayDataFilter -> sampleCategoricalAttributes.contains(genericAssayDataFilter.getAttributeId()))
            //                .collect(Collectors.toList()))
            // TODO: (required) get profile and filter the profiles
            List<String> sampleCategoricalProfileTypes = genericAssayProfilesMap.get(ClinicalAttributeDataSource.SAMPLE)
                .stream().filter(profile -> profile.getDatatype().equals("CATEGORICAL") || profile.getDatatype().equals("BINARY"))
                .map(profile -> profile.getStableId().replace(profile.getCancerStudyIdentifier() + "_", ""))
                .toList();

            List<String> sampleNumericalProfileTypes = genericAssayProfilesMap.get(ClinicalAttributeDataSource.SAMPLE)
                .stream().filter(profile -> profile.getDatatype().equals("LIMIT-VALUE"))
                .map(profile -> profile.getStableId().replace(profile.getCancerStudyIdentifier() + "_", ""))
                .toList();
            builder.setSampleNumericalGenericAssayDataFilters(studyViewFilter.getGenericAssayDataFilters().stream()
                .filter(genericAssayDataFilter -> sampleNumericalProfileTypes.contains(genericAssayDataFilter.getProfileType()))
                .collect(Collectors.toList()));
            builder.setSampleCategoricalGenericAssayDataFilters(studyViewFilter.getGenericAssayDataFilters().stream()
                .filter(genericAssayDataFilter -> sampleCategoricalProfileTypes.contains(genericAssayDataFilter.getProfileType()))
                .collect(Collectors.toList()));
        }
        return builder.build();
    }
    
}