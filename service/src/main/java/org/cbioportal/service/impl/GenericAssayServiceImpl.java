package org.cbioportal.service.impl;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.cbioportal.model.GenericAssayAdditionalProperty;
import org.cbioportal.model.GenericAssayData;
import org.cbioportal.model.GenericAssayMolecularAlteration;
import org.cbioportal.model.MolecularProfile;
import org.cbioportal.model.MolecularProfile.MolecularAlterationType;
import org.cbioportal.model.MolecularProfileSamples;
import org.cbioportal.model.Sample;
import org.cbioportal.model.meta.GenericAssayMeta;
import org.cbioportal.persistence.GenericAssayRepository;
import org.cbioportal.persistence.MolecularDataRepository;
import org.cbioportal.persistence.SampleListRepository;
import org.cbioportal.service.GenericAssayService;
import org.cbioportal.service.MolecularProfileService;
import org.cbioportal.service.SampleService;
import org.cbioportal.service.exception.MolecularProfileNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


@Service
public class GenericAssayServiceImpl implements GenericAssayService {

    @Autowired
    private GenericAssayRepository genericAssayRepository;

    @Autowired
    private MolecularDataRepository molecularDataRepository;

    @Autowired
    private SampleService sampleService;

    @Autowired
    private MolecularProfileService molecularProfileService;

    @Autowired
    private SampleListRepository sampleListRepository;

    @Override
    public List<GenericAssayMeta> getGenericAssayMetaByStableIdsAndMolecularIds(List<String> stableIds, List<String> molecularProfileIds, String projection) {
        List<GenericAssayMeta> metaResults = new ArrayList<>();
        
        // fetch meta from stable ids, this is the rare case
        if (stableIds != null) {
            Set<String> allStableIdsFromProfile = new HashSet<>();
            if (molecularProfileIds != null) {
                List<String> distinctMolecularProfileIds = molecularProfileIds.stream().distinct().sorted().collect(Collectors.toList());
                if (distinctMolecularProfileIds.size() > 0) {
                    // fetch one profile at a time to improve cache performace for multiple profiles query
                    for (String distinctMolecularProfileId : distinctMolecularProfileIds) {
                        allStableIdsFromProfile.addAll(genericAssayRepository.getGenericAssayStableIdsByMolecularIds(Arrays.asList(distinctMolecularProfileId)));
                    }
                }
            }
            List<String> commonStableIds;
            List<String> allDistinctStableIdsFromProfile= new ArrayList<>(allStableIdsFromProfile);
            if (allDistinctStableIdsFromProfile.size() > 0) {
                commonStableIds = allDistinctStableIdsFromProfile.stream()
                    .distinct()
                    .filter(stableIds::contains)
                    .collect(Collectors.toList());            
            } else {
                commonStableIds = stableIds;
            }
            if (commonStableIds.size() > 0) {
                Map<String, List<GenericAssayAdditionalProperty>> additionalPropertiesGroupedByStableId =
                    genericAssayRepository.getGenericAssayAdditionalproperties(commonStableIds).stream()
                        .collect(Collectors.groupingBy(GenericAssayAdditionalProperty::getStableId));

                for (String stableId : commonStableIds) {
                    GenericAssayMeta meta = new GenericAssayMeta(stableId);
                    HashMap<String, String> map = new HashMap<>();
                    if (additionalPropertiesGroupedByStableId.containsKey(stableId)) {
                        for (GenericAssayAdditionalProperty additionalProperty : additionalPropertiesGroupedByStableId.get(stableId)) {
                            map.put(additionalProperty.getName(), additionalProperty.getValue());
                        }
                    }
                    meta.setEntityType("GENERIC_ASSAY");
                    meta.setGenericEntityMetaProperties(map);
                    metaResults.add(meta);
                }
            }
        } else {
            // fetch meta from molecular profile ids, this is the common case
            List<String> distinctMolecularProfileIds = molecularProfileIds.stream().distinct().sorted().collect(Collectors.toList());
            if (distinctMolecularProfileIds.size() > 0) {
                // fetch one profile at a time to improve cache performance for multiple profiles query
                Set<GenericAssayAdditionalProperty> allAdditionalPropertiesGroupedByStableId = new HashSet<>();
                for (String distinctMolecularProfileId : distinctMolecularProfileIds) {
                    allAdditionalPropertiesGroupedByStableId.addAll(genericAssayRepository.getGenericAssayAdditionalpropertiesByMolecularProfileId(distinctMolecularProfileId));
                }
                List<GenericAssayAdditionalProperty> distinctAdditionalPropertiesGroupedByStableIds = new ArrayList<>(allAdditionalPropertiesGroupedByStableId);
                Map<String, List<GenericAssayAdditionalProperty>> additionalPropertiesGroupedByStableId = distinctAdditionalPropertiesGroupedByStableIds.stream()
                    .collect(Collectors.groupingBy(GenericAssayAdditionalProperty::getStableId));

                for (Map.Entry<String, List<GenericAssayAdditionalProperty>> entry : additionalPropertiesGroupedByStableId.entrySet()) {
                    GenericAssayMeta meta = new GenericAssayMeta(entry.getKey());
                    HashMap<String, String> map = new HashMap<>();
                    if (entry.getValue() != null && entry.getValue().size() > 0) {
                        for (GenericAssayAdditionalProperty additionalProperty : entry.getValue()) {
                            map.put(additionalProperty.getName(), additionalProperty.getValue());
                        }
                    }
                    meta.setEntityType("GENERIC_ASSAY");
                    meta.setGenericEntityMetaProperties(map);
                    metaResults.add(meta);
                }
            }
        }

        return metaResults;
    }

    @Override
    public List<GenericAssayData> fetchGenericAssayData(List<String> molecularProfileIds, 
    List<String> sampleIds, List<String> genericAssayStableIds, String projection) throws MolecularProfileNotFoundException {
        List<GenericAssayData> result = new ArrayList<>();

        SortedSet<String> distinctMolecularProfileIds = new TreeSet<>(molecularProfileIds);

        Map<String, MolecularProfileSamples> commaSeparatedSampleIdsOfMolecularProfilesMap = molecularDataRepository
                .commaSeparatedSampleIdsOfMolecularProfilesMap(distinctMolecularProfileIds);

        Map<String, Map<Integer, Integer>> internalSampleIdsMap = new HashMap<>();
        List<Integer> allInternalSampleIds = new ArrayList<>();

        for (String molecularProfileId : distinctMolecularProfileIds) {
            List<Integer> internalSampleIds = Arrays
                    .stream(commaSeparatedSampleIdsOfMolecularProfilesMap.get(molecularProfileId).getSplitSampleIds())
                    .mapToInt(Integer::parseInt).boxed().collect(Collectors.toList());
            HashMap<Integer, Integer> molecularProfileSampleMap = new HashMap<Integer, Integer>();
            for (int lc = 0; lc < internalSampleIds.size(); lc++) {
                molecularProfileSampleMap.put(internalSampleIds.get(lc), lc);
            }
            internalSampleIdsMap.put(molecularProfileId, molecularProfileSampleMap);
            allInternalSampleIds.addAll(internalSampleIds);
        }
    
        List<MolecularProfile> molecularProfiles = new ArrayList<>();
        List<MolecularProfile> distinctMolecularProfiles = molecularProfileService.getMolecularProfiles(
            distinctMolecularProfileIds, "SUMMARY");
        Map<String, MolecularProfile> molecularProfileMapById = distinctMolecularProfiles.stream().collect(
            Collectors.toMap(MolecularProfile::getStableId, Function.identity()));
        Map<String, List<MolecularProfile>> molecularProfileMapByStudyId = distinctMolecularProfiles.stream().collect(
            Collectors.groupingBy(MolecularProfile::getCancerStudyIdentifier));
        List<Sample> samples;
        if (sampleIds == null) {
            samples = sampleService.getSamplesByInternalIds(allInternalSampleIds);
            for (String molecularProfileId : distinctMolecularProfileIds) {
                internalSampleIdsMap.get(molecularProfileId).keySet().forEach(s -> molecularProfiles.add(molecularProfileMapById
                    .get(molecularProfileId)));
            }
        } else {
            for (String molecularProfileId : molecularProfileIds) {
                molecularProfiles.add(molecularProfileMapById.get(molecularProfileId));
            }
            List<String> studyIds = molecularProfiles.stream().map(MolecularProfile::getCancerStudyIdentifier)
                .collect(Collectors.toList());
            samples = sampleService.fetchSamples(studyIds, sampleIds, "ID");
        }
    
        List<GenericAssayMolecularAlteration> molecularAlterations = new ArrayList<>();
        for (String distinctMolecularProfileId : distinctMolecularProfileIds) {
            molecularAlterations.addAll(molecularDataRepository.getGenericAssayMolecularAlterations(
                distinctMolecularProfileId, genericAssayStableIds, projection));
        }
        Map<String, List<GenericAssayMolecularAlteration>> molecularAlterationsMap = molecularAlterations.stream().collect(
            Collectors.groupingBy(GenericAssayMolecularAlteration::getMolecularProfileId));
        
        for (Sample sample : samples) {
            for (MolecularProfile molecularProfile : molecularProfileMapByStudyId.get(sample.getCancerStudyIdentifier())) {
                String molecularProfileId = molecularProfile.getStableId();
                Integer indexOfSampleId = internalSampleIdsMap.get(molecularProfileId).get(sample.getInternalId());
                if (indexOfSampleId != null && molecularAlterationsMap.containsKey(molecularProfileId)) {
                    for (GenericAssayMolecularAlteration molecularAlteration : molecularAlterationsMap.get(molecularProfileId)) {
                        GenericAssayData molecularData = new GenericAssayData();
                        molecularData.setMolecularProfileId(molecularProfileId);
                        molecularData.setSampleId(sample.getStableId());
                        molecularData.setPatientId(sample.getPatientStableId());
                        molecularData.setStudyId(sample.getCancerStudyIdentifier());
                        molecularData.setGenericAssayStableId(molecularAlteration.getGenericAssayStableId());
                        molecularData.setValue(molecularAlteration.getSplitValues()[indexOfSampleId]);
                        result.add(molecularData);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public List<GenericAssayData> getGenericAssayData(String molecularProfileId, String sampleListId,
                                                    List<String> genericAssayStableIds, String projection)
        throws MolecularProfileNotFoundException {
        
        validateMolecularProfile(molecularProfileId);
        List<String> sampleIds = sampleListRepository.getAllSampleIdsInSampleList(sampleListId);
        if (sampleIds.isEmpty()) {
            return Collections.emptyList();
        }
        return fetchGenericAssayData(Arrays.asList(molecularProfileId), sampleIds, genericAssayStableIds, projection);
    }

    @Override
    public List<GenericAssayData> fetchGenericAssayData(String molecularProfileId, List<String> sampleIds,
            List<String> genericAssayStableIds, String projection) throws MolecularProfileNotFoundException {

        validateMolecularProfile(molecularProfileId);
        return fetchGenericAssayData(Arrays.asList(molecularProfileId), sampleIds, genericAssayStableIds, projection);
    }

    private void validateMolecularProfile(String molecularProfileId) throws MolecularProfileNotFoundException {

        MolecularProfile molecularProfile = molecularProfileService.getMolecularProfile(molecularProfileId);

        if (!molecularProfile.getMolecularAlterationType().equals(MolecularAlterationType.GENERIC_ASSAY)) {

            throw new MolecularProfileNotFoundException(molecularProfileId);
        }
    }
}