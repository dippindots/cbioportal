package org.cbioportal.persistence;

import java.util.List;

import org.cbioportal.model.GeneMolecularAlteration;
import org.cbioportal.model.GenesetMolecularAlteration;
import org.cbioportal.model.TreatmentMolecularAlteration;
import org.cbioportal.model.GenericAssayMolecularAlteration;

public interface MolecularDataRepository {
    
    String getCommaSeparatedSampleIdsOfMolecularProfile(String molecularProfileId);
    
    List<String> getCommaSeparatedSampleIdsOfMolecularProfiles(List<String> molecularProfileIds);
    
    List<GeneMolecularAlteration> getGeneMolecularAlterations(String molecularProfileId,
        List<Integer> entrezGeneIds, String projection);
    
    List<GeneMolecularAlteration> getGeneMolecularAlterationsInMultipleMolecularProfiles(List<String> molecularProfileIds, 
        List<Integer> entrezGeneIds, String projection);
    
    List<GenesetMolecularAlteration> getGenesetMolecularAlterations(String molecularProfileId,
        List<String> genesetIds, String projection);
    
    List<TreatmentMolecularAlteration> getTreatmentMolecularAlterations(String molecularProfileId,
        List<String> treatmentIds, String projection);

    List<GenericAssayMolecularAlteration> getGenericAssayMolecularAlterations(String molecularProfileId, List<Integer> stableIds,
        String projection);
}
