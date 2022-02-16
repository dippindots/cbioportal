/*
 * Copyright (c) 2015 - 2016 Memorial Sloan-Kettering Cancer Center.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY, WITHOUT EVEN THE IMPLIED WARRANTY OF MERCHANTABILITY OR FITNESS
 * FOR A PARTICULAR PURPOSE. The software and documentation provided hereunder
 * is on an "as is" basis, and Memorial Sloan-Kettering Cancer Center has no
 * obligations to provide maintenance, support, updates, enhancements or
 * modifications. In no event shall Memorial Sloan-Kettering Cancer Center be
 * liable to any party for direct, indirect, special, incidental or
 * consequential damages, including lost profits, arising out of the use of this
 * software and its documentation, even if Memorial Sloan-Kettering Cancer
 * Center has been advised of the possibility of such damage.
 */

/*
 * This file is part of cBioPortal.
 *
 * cBioPortal is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package org.mskcc.cbio.portal.web_api;

import java.io.IOException;
import java.util.*;
import org.json.simple.JSONArray;
import org.mskcc.cbio.io.WebFileConnect;
import org.mskcc.cbio.portal.dao.*;
import org.mskcc.cbio.portal.model.*;
import org.mskcc.cbio.portal.servlet.WebService;
import org.mskcc.cbio.portal.util.*;

/**
 * Web Service to Get Profile Data.
 *
 * @author Ethan Cerami.
 */
public class GetProfileData {
    public static final int ID_ENTREZ_GENE = 1;
    public static final int GENE_SYMBOL = 0;
    private String rawContent;
    private String[][] matrix;
    private ProfileData profileData;
    private List<String> warningList = new ArrayList<String>();

    /**
     * Constructor.
     * @param targetGeneticProfileIdList    Target Genetic Profile List.
     * @param targetGeneList                Target Gene List.
     * @param targetSampleList             Target Sample List.
     * @param suppressMondrianHeader        Flag to suppress the mondrian header.
     * @throws DaoException Database Error.
     * @throws IOException IO Error.
     */
    public GetProfileData (List<String> targetGeneticProfileIdList,
            List<String> targetGeneList,
            List<String> targetSampleList,
            Boolean suppressMondrianHeader)
            throws DaoException, IOException {
        execute(targetGeneticProfileIdList, targetGeneList, targetSampleList, suppressMondrianHeader);
    }

    /**
     * Constructor.
     *
     * @param geneticProfile    Genetic Profile Object.
     * @param targetGeneList    Target Gene List.
     * @param sampleIds        White-space delimited sample IDs.
     * @throws DaoException     Database Error.
     * @throws IOException      IO Error.
     */
    public GetProfileData (GeneticProfile geneticProfile, List<String> targetGeneList,
            String sampleIds) throws DaoException, IOException {
        List<String> targetGeneticProfileIdList = new ArrayList<String>();
        targetGeneticProfileIdList.add(geneticProfile.getStableId());

        List<String> targetSampleList = new ArrayList<String>();
        String sampleIdParts[] = sampleIds.split("\\s+");
        for (String sampleIdPart : sampleIdParts) {
            targetSampleList.add(sampleIdPart);
        }
        execute(targetGeneticProfileIdList, targetGeneList, targetSampleList, true);
    }

    /**
     * Gets the Raw Content Generated by the Web API.
     * @return Raw Content.
     */
    public String getRawContent() {
        return rawContent;
    }

    /**
     * Gets the Data Matrix Generated by the Web API.
     * @return Matrix of Strings.
     */
    public String[][] getMatrix() {
        return matrix;
    }

    /**
     * Gets the Profile Data Object Generated by the Web API.
     * @return ProfileData Object.
     */
    public ProfileData getProfileData() {
        return profileData;
    }

    /**
     * Gets warnings (if triggered).
     *
     * @return List of Warning Strings.
     */
    public List<String> getWarnings() {
        return this.warningList;
    }

    /**
     * Executes the LookUp.
     */
    private void execute(List<String> targetGeneticProfileIdList,
            List<String> targetGeneList, List<String> targetSampleList,
            Boolean suppressMondrianHeader) throws DaoException, IOException {
        this.rawContent = getProfileData (targetGeneticProfileIdList, targetGeneList,
                targetSampleList, suppressMondrianHeader);
        this.matrix = WebFileConnect.parseMatrix(rawContent);

        //  Create the Profile Data Object
        if (targetGeneticProfileIdList.size() == 1) {
            String geneticProfileId = targetGeneticProfileIdList.get(0);
            GeneticProfile geneticProfile =
                    DaoGeneticProfile.getGeneticProfileByStableId(geneticProfileId);
            profileData = new ProfileData(geneticProfile, matrix);
        }
    }

    /**
     * Gets Profile Data for Specified Target Info.
     * @param targetGeneticProfileIdList    Target Genetic Profile List.
     * @param targetGeneList                Target Gene List.
     * @param targetSampleList             Target Sample List.
     * @param suppressMondrianHeader        Flag to suppress the mondrian header.
     * @return Tab Delim Text String.
     * @throws DaoException Database Error.
     */
    private String getProfileData(List<String> targetGeneticProfileIdList,
            List<String> targetGeneList, 
            List<String> targetSampleList,
            Boolean suppressMondrianHeader) throws DaoException {

        StringBuffer buf = new StringBuffer();

        //  Validate that all Genetic Profiles are valid Stable IDs.
        for (String geneticProfileId:  targetGeneticProfileIdList) {
            GeneticProfile geneticProfile =
                    DaoGeneticProfile.getGeneticProfileByStableId(geneticProfileId);
            if (geneticProfile == null) {
                buf.append("No genetic profile available for " + WebService.GENETIC_PROFILE_ID + ":  ")
                        .append(geneticProfileId).append(".").append (WebApiUtil.NEW_LINE);
                return buf.toString();
            }
        }
        // validate all genetic profiles belong to the same cancer study
        if (differentCancerStudies(targetGeneticProfileIdList)) {
            buf.append("Genetic profiles must come from same cancer study.").append((WebApiUtil.NEW_LINE));
            return buf.toString();
        }
        int cancerStudyId = DaoGeneticProfile.getGeneticProfileByStableId(targetGeneticProfileIdList.get(0)).getCancerStudyId();
        List<Integer> internalSampleIds = InternalIdUtil.getInternalSampleIds(cancerStudyId, targetSampleList);

        //  Branch based on number of profiles requested.
        //  In the first case, we have 1 profile and 1 or more genes.
        //  In the second case, we have > 1 profiles and only 1 gene.
        if (targetGeneticProfileIdList.size() == 1) {
            String geneticProfileId = targetGeneticProfileIdList.get(0);
            GeneticProfile geneticProfile = DaoGeneticProfile.getGeneticProfileByStableId(geneticProfileId);

            //  Get the Gene List
            List<Gene> geneList = WebApiUtil.getGeneList(targetGeneList,
                    geneticProfile.getGeneticAlterationType(), buf, warningList);
            
            //  Output DATA_TYPE and COLOR_GRADIENT_SETTINGS (Used by Mondrian Cytoscape PlugIn)
            if (!suppressMondrianHeader) {
                buf.append("# DATA_TYPE\t ").append(geneticProfile.getProfileName()).append ("\n");
                buf.append("# COLOR_GRADIENT_SETTINGS\t ").append(geneticProfile.getGeneticAlterationType().name())
                        .append ("\n");
            }

            //  Ouput Column Headings
            buf.append ("GENE_ID\tCOMMON");
            outputRow(targetSampleList, buf);

            //  Iterate through all validated genes, and extract profile data.
            for (Gene gene: geneList) {                
                List<String> dataRow = GeneticAlterationUtil.getGeneticAlterationDataRow(gene,
                        internalSampleIds, geneticProfile);
                outputGeneRow(dataRow, gene, buf);
            }
        } else {
            //  Ouput Column Headings
            buf.append ("GENETIC_PROFILE_ID\tALTERATION_TYPE\tGENE_ID\tCOMMON");
            outputRow(targetSampleList, buf);
            
            List<GeneticProfile> profiles = new ArrayList<GeneticProfile>(targetGeneticProfileIdList.size());
            boolean includeRPPAProteinLevel = false;
            for (String gId:  targetGeneticProfileIdList) {
                GeneticProfile profile = DaoGeneticProfile.getGeneticProfileByStableId(gId);
                profiles.add(profile);
                if (profile.getGeneticAlterationType() == GeneticAlterationType.PROTEIN_ARRAY_PROTEIN_LEVEL) {
                    includeRPPAProteinLevel = true;
                }
            }

            // for rppa protein level, choose the best correlated one
            if (includeRPPAProteinLevel && profiles.size()==2) {
                GeneticProfile gp1, gp2;
                if (profiles.get(0).getGeneticAlterationType() == GeneticAlterationType.PROTEIN_ARRAY_PROTEIN_LEVEL) {
                    gp1 = profiles.get(1);
                    gp2 = profiles.get(0);
                } else {
                    gp1 = profiles.get(0);
                    gp2 = profiles.get(1);
                }
                
                // get data of the other profile
                List<String> dataRow1 = null;
                List<Gene> geneList = WebApiUtil.getGeneList(targetGeneList,
                        gp1.getGeneticAlterationType(), buf, warningList);
                
                if (geneList.size() > 0) {
                    Gene gene = geneList.get(0);
                    buf.append(gp1.getStableId()).append(WebApiUtil.TAB).append(gp1.getGeneticAlterationType().name())
                            .append (WebApiUtil.TAB);   
                    dataRow1 = GeneticAlterationUtil.getGeneticAlterationDataRow(gene,
                            internalSampleIds, gp1);
                    outputGeneRow(dataRow1, gene, buf);
                }
            } else {            
                //  Iterate through all genetic profiles
                for (GeneticProfile geneticProfile : profiles) {
                    //  Get the Gene List
                    List<Gene> geneList = WebApiUtil.getGeneList(targetGeneList,
                            geneticProfile.getGeneticAlterationType(), buf, warningList);

                    if (geneList.size() > 0) {
                        Gene gene = geneList.get(0);
                        buf.append(geneticProfile.getStableId()).append(WebApiUtil.TAB)
                                .append(geneticProfile.getGeneticAlterationType().name()).append(WebApiUtil.TAB);   
                        List<String> dataRow = GeneticAlterationUtil.getGeneticAlterationDataRow(gene,
                                internalSampleIds, geneticProfile);
                        outputGeneRow(dataRow, gene, buf);
                    }
                }
            }
        }
        return buf.toString();
    }

    private static boolean differentCancerStudies(List<String> targetGeneticProfileList)
    {
        if (targetGeneticProfileList.size() == 1) return false;

        int firstCancerStudyId = -1;
        boolean processingFirstId = true;
        for (String profileId : targetGeneticProfileList) {
            GeneticProfile p = DaoGeneticProfile.getGeneticProfileByStableId(profileId);
          if (processingFirstId) {
                firstCancerStudyId = p.getCancerStudyId();
                processingFirstId = false;
          }  
          else if (p.getCancerStudyId() != firstCancerStudyId) {
              return true;
          }
        }
        return false;
    }

    private static void outputRow(List<String> dataValues, StringBuffer buf) {
        for (String value:  dataValues) {
            buf.append(WebApiUtil.TAB).append (value);
        }
        buf.append (WebApiUtil.NEW_LINE);
    }

    private static void outputGeneRow(List<String> dataRow, Gene gene, StringBuffer buf)
            throws DaoException {
        if (gene instanceof CanonicalGene) {
            CanonicalGene canonicalGene = (CanonicalGene) gene;
            buf.append(canonicalGene.getEntrezGeneId()).append (WebApiUtil.TAB);
            buf.append (canonicalGene.getHugoGeneSymbolAllCaps());
        } else if (gene instanceof MicroRna) {
            MicroRna microRna = (MicroRna) gene;
            buf.append("-999999").append (WebApiUtil.TAB);
            buf.append (microRna.getMicroRnaId());
        }
        outputRow (dataRow, buf);
    }

    public JSONArray getJson() {
        JSONArray toReturn = new JSONArray();


        List<String> sampleIds = new ArrayList<String>(Arrays.asList(matrix[0]));
        sampleIds.subList(0,4).clear();       // remove column names and meta data

        for (String s : sampleIds) {
            toReturn.add(s);
        }

//        for (String s : matrix[1]) {
//            toReturn.add(s);
//        }

        return toReturn;
    }
}
