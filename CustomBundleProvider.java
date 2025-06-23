package ca.uhn.fhir.jpa.starter.custom;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.Date;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.AllergyIntolerance;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Composition;
import org.hl7.fhir.r4.model.ContactPoint.ContactPointSystem;
import org.hl7.fhir.r4.model.ContactPoint.ContactPointUse;
import org.hl7.fhir.r4.model.Device;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Immunization;
import org.hl7.fhir.r4.model.Medication;
import org.hl7.fhir.r4.model.MedicationAdministration;
import org.hl7.fhir.r4.model.MedicationDispense;
import org.hl7.fhir.r4.model.MedicationRequest;
import org.hl7.fhir.r4.model.MedicationStatement;
import org.hl7.fhir.r4.model.Meta;
import org.hl7.fhir.r4.model.Narrative;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Organization;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Practitioner;
import org.hl7.fhir.r4.model.Procedure;
import org.hl7.fhir.r4.model.Provenance;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.RelatedPerson;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.Composition.SectionComponent;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Device.DeviceDeviceNameComponent;
import org.hl7.fhir.r4.model.Goal;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import ca.uhn.fhir.jpa.api.dao.IFhirResourceDao;
import ca.uhn.fhir.jpa.api.dao.IFhirResourceDaoPatient;
import ca.uhn.fhir.jpa.searchparam.SearchParameterMap;
import ca.uhn.fhir.rest.annotation.OptionalParam;
import ca.uhn.fhir.rest.annotation.RequiredParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.param.CompositeOrListParam;
import ca.uhn.fhir.rest.param.CompositeParam;
import ca.uhn.fhir.rest.param.DateParam;
import ca.uhn.fhir.rest.param.StringParam;
import ca.uhn.fhir.rest.param.TokenParam;
import ca.uhn.fhir.rest.server.IResourceProvider;

@Component
public class CustomBundleProvider implements IResourceProvider {

    private static final String excludedSystem = "http://myportal.org";

    @Autowired
    private IFhirResourceDaoPatient<Patient> patientDao;

    @Autowired
    private IFhirResourceDao<Bundle> bundleDao;

    @Override
    public Class<Bundle> getResourceType() {
        return Bundle.class;
    }


    @Search(queryName = "findContentByPatient")
    public List<IBaseResource> discoverContentByPatient(
        @RequiredParam(name = "patient.identifier") TokenParam patientIdentifier,
        @RequiredParam(name = "patient.birthdate") DateParam patientBirthdate,
        @RequiredParam(name = "patient.family") StringParam patientFamily,
        @RequiredParam(name = "patient.gender") TokenParam patientGender,
        @RequiredParam(name = "content-code") TokenParam contentCode,
        @OptionalParam(
            name = "section-lookback",
            compositeTypes = {TokenParam.class, DateParam.class})
            CompositeOrListParam<TokenParam, DateParam> sectionsWithLookback,
        RequestDetails requestDetails) {
                
        List<IBaseResource> retVal = new ArrayList<>();

        // Create a List to hold key-value pairs of sectionCode and lookbackDate
        List<Map.Entry<TokenParam, DateParam>> sectionLookbacks = new ArrayList<>();

        // Check if sectionsWithLookback is not null and has values
        if (sectionsWithLookback != null && sectionsWithLookback.size() != 0) {
            // If no sections are provided, we can return an empty list or handle it as needed 
            for (CompositeParam<TokenParam, DateParam> nextOr : sectionsWithLookback.getValuesAsQueryTokens()) {
                TokenParam sectionCode = nextOr.getLeftValue();
                DateParam lookbackDate = nextOr.getRightValue();
                sectionLookbacks.add(Map.entry(sectionCode, lookbackDate));
            }
        }

        // validate patient identity
        List<Patient> patientResources = searchPatient(patientIdentifier, patientBirthdate, patientFamily,
        patientGender, requestDetails);

        if (patientResources.isEmpty()) {
            // Handle case where no patient is found
            return null;
        } else if (patientResources.size() > 1) {
            // Create an operation outcome with error for multiple patients
            OperationOutcome operationOutcome = (generateOperationOutcome(OperationOutcome.IssueSeverity.ERROR,
                    OperationOutcome.IssueType.PROCESSING, "Multiple patients found with the given criteria."));           
            retVal.add(operationOutcome);
            return retVal;
        }

        Patient currPatient = patientResources.get(0);

        // check if contentType is for Patient Summary
        if (contentCode.getSystem().equals("http://loinc.org") && 
            contentCode.getValue().equals("60591-5")) {
            // Create a Patient Summary Bundle
            Bundle patientSummaryBundle = generateMHRPS(currPatient, sectionLookbacks, requestDetails);
            retVal.add(patientSummaryBundle);
        } else {
            // Handle other content types by searching
        }


        return retVal;
    }

    

    /**
     * Searches for a Patient resource based on the provided parameters.
     * 
     * @param patientIdentifier The identifier of the patient.
     * @param patientBirthdate The birthdate of the patient.
     * @param patientFamily The family name of the patient.
     * @param patientGender 
     * @param requestDetails The request details for the search.
     * @return the list of matching Patient resources.
     * */
    private List<Patient> searchPatient(TokenParam patientIdentifier, DateParam patientBirthdate,
            StringParam patientFamily, TokenParam patientGender, RequestDetails requestDetails) {

        SearchParameterMap patientParams = new SearchParameterMap();
        patientParams.add(Patient.SP_IDENTIFIER, new TokenParam(patientIdentifier.getSystem(), patientIdentifier.getValue()));
        patientParams.add(Patient.SP_BIRTHDATE, patientBirthdate);
        patientParams.add(Patient.SP_FAMILY, patientFamily);
        patientParams.add(Patient.SP_GENDER, patientGender);
        List<Patient> searchOutcome = patientDao.searchForResources(patientParams, requestDetails);

        return searchOutcome;
    }


    /**
     * Generates an OperationOutcome with the specified severity, issue type, and diagnostic message.
     * 
     * @param severity The severity of the issue (e.g., ERROR, WARNING).
     * @param issueType The type of issue (e.g., PROCESSING, NOTFOUND).
     * @param diagnostic A diagnostic message describing the issue.
     * @return An OperationOutcome resource containing the issue details.
     */
    private OperationOutcome generateOperationOutcome(OperationOutcome.IssueSeverity severity,
            OperationOutcome.IssueType issueType, String diagnostic) {
        OperationOutcome operationOutcome = new OperationOutcome();
        operationOutcome.addIssue().setSeverity(severity).setCode(issueType).setDiagnostics(diagnostic);

        return operationOutcome;
    }


    /**
     * Generates a My Health Record Patient Summary Bundle (MHR PS) for the given patient.
     * 
     * @param patient The Patient resource for which the summary is generated.
     * @param sectionLookbacks A list of section codes and their lookback dates.
     * @param requestDetails The request details for the operation.
     * @return A Bundle containing the MHR PS resources.
     */
    private Bundle generateMHRPS(Patient patient, List<Map.Entry<TokenParam, DateParam>> sectionLookbacks, RequestDetails requestDetails) {
        Bundle mhrPsBundle = new Bundle();
            
        // Set the Bundle ID to a unique identifier
        String bundleId = UUID.randomUUID().toString();
        mhrPsBundle.setId(bundleId);

        // Set the meta profile for the bundle
        mhrPsBundle.setMeta(new Meta()
            .addProfile("http://ns.electronichealth.net.au/fhir/mhr/ps/sparked-testing/StructureDefinition/mhr-au-ps-bundle"));

        // set identifier for the Bundle
        String bundleIdentifier = UUID.randomUUID().toString();
        mhrPsBundle.setIdentifier(new Identifier()
            .setSystem("http://mhr-operator/fhir/identifier")
            .setValue(bundleIdentifier)
        );

        // Set the Bundle type to DOCUMENT
        mhrPsBundle.setType(Bundle.BundleType.DOCUMENT);

        // Set the Bundle timestamp
        mhrPsBundle.setTimestamp(new Date());

        // --- Generate UUIDs for resources ---
        String patientUuid = UUID.randomUUID().toString();
        String authorDeviceUuid = UUID.randomUUID().toString();
        String authorOrgUuid = UUID.randomUUID().toString();
        String compositionUuid = UUID.randomUUID().toString();

        // --- Add Composition resource to the Bundle ---
        Composition composition = new Composition();
        composition.setId(compositionUuid);

        // status
        composition.setStatus(Composition.CompositionStatus.FINAL);

        // Type
        composition.setType(new CodeableConcept().addCoding(
            new Coding()
                .setSystem("http://loinc.org")
                .setCode("60591-5")
        ));

        // Subject (Patient reference)
        composition.setSubject(new Reference("urn:uuid:" + patientUuid));

        // Date
        composition.setDate(new Date());

        // Author (Device)
        composition.addAuthor(new Reference("urn:uuid:" + authorDeviceUuid));

        // Custodian (Organization)
        composition.setCustodian(new Reference("urn:uuid:" + authorOrgUuid));

        // Title
        // Format: dd-MMMM-yyyy HH:mm a z (e.g., 10-June-2025 08:38 am AEST)
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MMMM-yyyy hh:mm a z");
        sdf.setTimeZone(TimeZone.getTimeZone("Australia/Sydney"));
        String formattedDate = sdf.format(new Date());
        composition.setTitle("MHR Generated Patient Summary - " + formattedDate);

        // Add Composition to Bundle
        Bundle.BundleEntryComponent compositionEntry = mhrPsBundle.addEntry();
        compositionEntry.setFullUrl("urn:uuid:" + compositionUuid);
        compositionEntry.setResource(composition);

        // --- Add Patient resource ---
        // Generate narrative for the patient
        patient.setText(generatePatientNarrative(patient));
        patient.setMeta(new Meta()
            .addProfile("http://ns.electronichealth.net.au/fhir/mhr/ps/sparked-testing/StructureDefinition/mhr-au-ps-patient"));
        patient.setId(patientUuid);
        Bundle.BundleEntryComponent patientEntry = mhrPsBundle.addEntry();
        patientEntry.setFullUrl("urn:uuid:" + patientUuid);
        patientEntry.setResource(patient);

        // --- Add Device resource (author) ---
        Device authorDevice = createAuthoringDevice(authorDeviceUuid, authorOrgUuid);     
        Bundle.BundleEntryComponent deviceEntry = mhrPsBundle.addEntry();
        deviceEntry.setFullUrl("urn:uuid:" + authorDeviceUuid);
        deviceEntry.setResource(authorDevice);

        // --- Add Organization resource (author owner) ---
        Organization deviceOrg = createDeviceOrganization(authorOrgUuid);
        Bundle.BundleEntryComponent orgEntry = mhrPsBundle.addEntry();
        orgEntry.setFullUrl("urn:uuid:" + authorOrgUuid);
        orgEntry.setResource(deviceOrg);

        // populate sections

        //1. find all the documents related to the patient
        List<Bundle> patientDocuments = retrieveDocumentsByPatient(patient, requestDetails); // RequestDetails can be passed if needed

        //2. mandatory sections

        //2.1 Problems section
        SectionComponent problemsSection = problemsSection(patientDocuments, mhrPsBundle, patientUuid);
        composition.addSection(problemsSection);

        //2.2 Allergies section
        SectionComponent allergiesSection = allergiesSection(patientDocuments, mhrPsBundle, patientUuid);   
        composition.addSection(allergiesSection);

        // 2.3 Medications section
        SectionComponent medicationsSection = medicationsSection(patientDocuments, mhrPsBundle, patientUuid);
        composition.addSection(medicationsSection);

        //3. optional sections
        if (sectionLookbacks != null && !sectionLookbacks.isEmpty()) {
            for (Map.Entry<TokenParam, DateParam> entry : sectionLookbacks) {
                TokenParam sectionCode = entry.getKey();
                

                // Handle each section based on the code
                if (sectionCode.getSystem().equals("http://loinc.org")
                    && sectionCode.getValue().equals("11369-6")) {
                    // default to 2 years lookback
                    Date lookbackDate = new Date(System.currentTimeMillis() - (2L * 365 * 24 * 60 * 60 * 1000));
                    if(entry.getValue() != null && entry.getValue().isEmpty() == false) { 
                        lookbackDate = entry.getValue().getValue();
                    }
                    // Call method to handle immunization section
                    SectionComponent immunizationSection = immunizationsSection(patientDocuments, mhrPsBundle, patientUuid, lookbackDate);
                    composition.addSection(immunizationSection);
                } else if (sectionCode.getSystem().equals("http://loinc.org")
                    && sectionCode.getValue().equals("47519-4")) {
                    // default to 5 years lookback
                    Date lookbackDate = new Date(System.currentTimeMillis() - (5L * 365 * 24 * 60 * 60 * 1000));
                    if(entry.getValue() != null && entry.getValue().isEmpty() == false) { 
                        lookbackDate = entry.getValue().getValue();
                    }
                    // Call method to handle procedure section
                    SectionComponent procedureHistorySection = procedureHistorySection(patientDocuments, mhrPsBundle, patientUuid, lookbackDate);
                    composition.addSection(procedureHistorySection);
                } else if (sectionCode.getSystem().equals("http://loinc.org")
                    && sectionCode.getValue().equals("81338-6")) {
                    // Call method to handle patient story section
                    SectionComponent patientStorySection = patientStorySection(patientDocuments, mhrPsBundle, patientUuid);
                    composition.addSection(patientStorySection);
                }
            }
        }
    

        return mhrPsBundle;
    }

    

    /**
     * Generates a Narrative for the Patient resource.
     * 
     * @param patient The Patient resource to generate the narrative for.
     * @return A Narrative object containing the generated narrative.
     */
    private Narrative generatePatientNarrative(Patient patient) {
        StringBuilder patientNarrative = new StringBuilder();
        patientNarrative.append("<div xmlns=\"http://www.w3.org/1999/xhtml\">");
        patientNarrative.append("<b>Name:</b> ");
        if (patient.hasName() && !patient.getName().isEmpty()) {
            patientNarrative.append(patient.getNameFirstRep().getNameAsSingleString());
        } else {
            patientNarrative.append("Unknown");
        }
        patientNarrative.append("<br/><b>Date of Birth:</b> ");
        if (patient.hasBirthDate()) {
            patientNarrative.append(new java.text.SimpleDateFormat("yyyy-MM-dd").format(patient.getBirthDate()));
        } else {
            patientNarrative.append("Unknown");
        }
        patientNarrative.append("<br/><b>Gender:</b> ");
        if (patient.hasGender()) {
            patientNarrative.append(patient.getGender().toCode());
        } else {
            patientNarrative.append("Unknown");
        }
        patientNarrative.append("</div>");
        Narrative narrative = new Narrative();
        narrative.setStatus(Narrative.NarrativeStatus.GENERATED);
        narrative.setDivAsString(patientNarrative.toString());
        return narrative;
    }

    /**
     * Creates a Device resource representing the authoring device.
     * 
     * @param deviceUuid The UUID for the device.
     * @param orgUuid The UUID for the organization that owns the device.
     * @return The created Device resource.
     */
    private Device createAuthoringDevice(String deviceUuid, String orgUuid) {
        Device device = new Device();
        device.setId(deviceUuid);
        device.addIdentifier(new Identifier()
            .setSystem("http://ns.electronichealth.net.au/id/pcehr/paid/1.0")
            .setValue("8003640003000026"));

        device.addDeviceName(new DeviceDeviceNameComponent()
            .setName("My Health Record")
            .setType(Device.DeviceNameType.MANUFACTURERNAME));
        device.setOwner(new Reference("urn:uuid:" + orgUuid));
        return device;
    }

    /**
     * Creates an Organization resource representing the device organization.
     * 
     * @param orgUuid The UUID for the organization.
     * @return The created Organization resource.
     */
    private Organization createDeviceOrganization(String orgUuid) {
        Organization deviceOrganization = new Organization();
        deviceOrganization.setId(orgUuid);
        deviceOrganization.addIdentifier(new Identifier()
            .setType(new CodeableConcept()
            .addCoding(new Coding()
                .setSystem("http://terminology.hl7.org/CodeSystem/v2-0203")
                .setCode("XX"))
            .setText("Australian Business Number (ABN)"))
            .setSystem("http://hl7.org.au/id/abn")
            .setValue("84425496912"));
        deviceOrganization.setName("My Health Record system operator");
        deviceOrganization.addTelecom()
            .setSystem(ContactPointSystem.EMAIL)
            .setValue("help@digitalhealth.gov.au")
            .setUse(ContactPointUse.WORK);
        deviceOrganization.addTelecom()
            .setSystem(ContactPointSystem.PHONE)
            .setValue("1300 901 001")
            .setUse(ContactPointUse.WORK);
        deviceOrganization.addAddress()
            .addLine("Level 25, 175 Liverpool Street")
            .setCity("Sydney")
            .setState("NSW")
            .setPostalCode("2000")
            .setCountry("Australia");

        return deviceOrganization;
    }
    

    /**
     * Retrieves document Bundles for a specific patient by searching for Bundles of type 'document'
     * where the first entry (Composition) references the given patient.
     */
    private List<Bundle> retrieveDocumentsByPatient(Patient patient, RequestDetails requestDetails) {
        SearchParameterMap bundleParams = new SearchParameterMap();
        // Restrict to document Bundles
        
        // Restrict to Bundles whose first entry Composition.subject references the patient
        // Find the IHI identifier from the patient's identifiers
        String ihiSystem = "http://ns.electronichealth.net.au/id/hi/ihi/1.0";
        String ihiValue = null;
        for (Identifier identifier : patient.getIdentifier()) {
            if (ihiSystem.equals(identifier.getSystem())) {
                ihiValue = identifier.getValue();
                break;
            }
        }
        if (ihiValue != null) {
            bundleParams.add(Bundle.SP_TYPE, new TokenParam("document"));
            bundleParams.add("composition.patient.identifier", new TokenParam(ihiSystem, ihiValue));
            return bundleDao.searchForResources(bundleParams, requestDetails);
        }
        return null;
    }

    /**
     * Creates a SectionComponent for the Medications section.
     * This is a placeholder method and should be implemented to retrieve actual medication data.
     */
    private SectionComponent allergiesSection(List<Bundle> patientDocuments, Bundle mhrPsBundle, String mhrPsPatientUuid) {
        SectionComponent section = new SectionComponent();
        Narrative sectionNarrative = new Narrative();
        sectionNarrative.setStatus(Narrative.NarrativeStatus.GENERATED);

        section.setTitle("Allergies and Intolerances");
        section.setCode(new CodeableConcept().addCoding(
            new Coding()
                .setSystem("http://loinc.org")
                .setCode("48765-2")
        ));

        boolean hasAllergies = false;
        if (patientDocuments != null) {
            for (Bundle document : patientDocuments) {
                if (document.hasIdentifier() && excludedSystem.equals(document.getIdentifier().getSystem())) continue;
                // Iterate through each entry in the document Bundle
                for (Bundle.BundleEntryComponent entry : document.getEntry()) {
                    IBaseResource resource = entry.getResource();
                    if (resource instanceof AllergyIntolerance) {
                        hasAllergies = true;
                        AllergyIntolerance allergy = (AllergyIntolerance) resource;
                        String allergyUuid = UUID.randomUUID().toString();

                        // Update the patient reference to the MHR PS Patient UUID
                        if (mhrPsPatientUuid != null) {
                            allergy.setPatient(new Reference("urn:uuid:" + mhrPsPatientUuid));
                        }

                        Reference allergyReference = new Reference("urn:uuid:" + allergyUuid);
                        section.addEntry(allergyReference);

                        // Add the allergy resource to the MHR PS Bundle
                        Bundle.BundleEntryComponent allergyEntry = mhrPsBundle.addEntry();
                        allergyEntry.setFullUrl("urn:uuid:" + allergyUuid);
                        allergy.setId(allergyUuid);
                        allergyEntry.setResource(allergy);

                        // --- Add Provenance resource for this AllergyIntolerance ---
                        generateProvenance(mhrPsBundle, document, allergy);

                        // Build up the table rows for each allergy
                        String existingDiv = sectionNarrative.hasDiv() ? sectionNarrative.getDivAsString() : null;
                        StringBuilder tableRows = new StringBuilder();

                        String allergyText = allergy.getCode() != null ? allergy.getCode().getText() : "";
                        String clinicalStatus = allergy.hasClinicalStatus() && allergy.getClinicalStatus().hasCoding()
                                ? allergy.getClinicalStatus().getCodingFirstRep().getCode() : "";
                        String verificationStatus = allergy.hasVerificationStatus() && allergy.getVerificationStatus().hasCoding()
                                ? allergy.getVerificationStatus().getCodingFirstRep().getCode() : "";
                        String onset = allergy.hasOnset() ? allergy.getOnset().toString() : "";

                        tableRows.append("<tr>")
                                .append("<td>").append(allergyText).append("</td>")
                                .append("<td>").append(clinicalStatus).append("</td>")
                                .append("<td>").append(verificationStatus).append("</td>")
                                .append("<td>").append(onset).append("</td>")
                                .append("</tr>");

                        if (existingDiv == null || existingDiv.isEmpty()) {
                            String tableHeader = "<div xmlns=\"http://www.w3.org/1999/xhtml\">" +
                                    "<table border=\"1\"><thead><tr>" +
                                    "<th>Allergy</th><th>Clinical Status</th><th>Verification Status</th><th>Onset</th>" +
                                    "</tr></thead><tbody>";
                            sectionNarrative.setDivAsString(tableHeader + tableRows.toString() + "</tbody></table></div>");
                        } else {
                            int insertPos = existingDiv.lastIndexOf("</tbody>");
                            if (insertPos != -1) {
                                String newDiv = existingDiv.substring(0, insertPos)
                                        + tableRows.toString()
                                        + existingDiv.substring(insertPos);
                                sectionNarrative.setDivAsString(newDiv);
                            }
                        }
                    }
                }
            }
        }

        if (!hasAllergies) {
            section.setEmptyReason(new CodeableConcept()
                .addCoding(new Coding()
                    .setSystem("http://terminology.hl7.org/CodeSystem/list-empty-reason")
                    .setCode("unavailable"))
                .setText("No information available."));
            sectionNarrative.setDivAsString("<div xmlns=\"http://www.w3.org/1999/xhtml\">No allergies or intolerances recorded.</div>");
        }

        section.setText(sectionNarrative);
        return section;
    }


    /**
     * Generates a Provenance resource for the given source document and target resource.
     * 
     * @param sourceDocument The source document Bundle containing the Composition.
     * @param provenanceTarget The target resource being provenanced (e.g., AllergyIntolerance).
     * @return void - it adds the generated Provenance to the document.
     */
    private void generateProvenance(Bundle mhrPsBundle, Bundle sourceDocument, IBaseResource provenanceTarget){
        Provenance provenance = new Provenance();
        String provenanceUuid = UUID.randomUUID().toString();
        provenance.setId(provenanceUuid);

        provenance.setRecorded(sourceDocument.getTimestamp() != null ? sourceDocument.getTimestamp() : new Date());

        // Set target to the resource being provenance
        if (provenanceTarget != null) {
            provenance.addTarget(new Reference("urn:uuid:" + provenanceTarget.getIdElement().getValue()));
        }

        Bundle.BundleEntryComponent provEntry = mhrPsBundle.addEntry();
        provEntry.setFullUrl("urn:uuid:" + provenanceUuid);
        provEntry.setResource(provenance);

        // Set agent (author of the document)
        // Add author agent (as before)
        if (sourceDocument != null && sourceDocument.getEntry() != null && !sourceDocument.getEntry().isEmpty()) {
            Bundle.BundleEntryComponent firstEntry = sourceDocument.getEntryFirstRep();
            if (firstEntry.getResource() instanceof Composition) {
            Composition comp = (Composition) firstEntry.getResource();
            // Author agent
            if (comp.hasAuthor() && !comp.getAuthor().isEmpty()) {
                Provenance.ProvenanceAgentComponent authorAgent = new Provenance.ProvenanceAgentComponent();
                authorAgent.setType(new CodeableConcept().addCoding(
                new Coding()
                    .setSystem("http://terminology.hl7.org/CodeSystem/provenance-participant-type")
                    .setCode("author")
                    .setDisplay("Author")
                ));

                provenance.addAgent(authorAgent);

                // find the author from the source document
                Reference authorRef = comp.getAuthorFirstRep();
                String authorRefId = authorRef.getReference();

                for (Bundle.BundleEntryComponent entry : sourceDocument.getEntry()) {
                    if (entry.getResource() != null && entry.getFullUrl().equals(authorRefId)) {

                        // check if the author resource is already present in mhrPsBundle
                        String provAuthor = findResourceFullUrlByIdentifier(entry.getResource(), mhrPsBundle);
                        if (provAuthor != null) {
                            authorAgent.setWho(new Reference(provAuthor));
                        } else {
                            authorAgent.setWho(authorRef);
                            Bundle.BundleEntryComponent newEntry = mhrPsBundle.addEntry();
                            newEntry.setFullUrl(authorRefId);
                            newEntry.setResource(entry.getResource());
                        }
                    }
                }
            }

            // Custodian agent
            if (comp.hasCustodian()) {
                Provenance.ProvenanceAgentComponent custodianAgent = new Provenance.ProvenanceAgentComponent();
                custodianAgent.setType(new CodeableConcept().addCoding(
                new Coding()
                    .setSystem("http://terminology.hl7.org/CodeSystem/provenance-participant-type")
                    .setCode("custodian")
                    .setDisplay("Custodian")
                ));

                provenance.addAgent(custodianAgent);

                // Fetch the actual custodian resource from the sourceDocument and add to mhrPsBundle
                Reference custodianRef = comp.getCustodian();
                String custodianRefId = custodianRef.getReference();

                for (Bundle.BundleEntryComponent entry : sourceDocument.getEntry()) {
                    if (entry.getResource() != null && entry.getFullUrl().equals(custodianRefId)) {

                        // check if the custodian resource is already present in mhrPsBundle
                        String provCustodian = findResourceFullUrlByIdentifier(entry.getResource(), mhrPsBundle);
                        if (provCustodian != null) {
                            custodianAgent.setWho(new Reference(provCustodian));
                        } else {
                            custodianAgent.setWho(custodianRef);
                            Bundle.BundleEntryComponent newEntry = mhrPsBundle.addEntry();
                            newEntry.setFullUrl(custodianRefId);
                            newEntry.setResource(entry.getResource());
                        }
                    }
                }
            }
            }
        }

        // Set entity (source document)
        if (sourceDocument != null) {
            Provenance.ProvenanceEntityComponent entity = new Provenance.ProvenanceEntityComponent();
            entity.setRole(Provenance.ProvenanceEntityRole.SOURCE);

            Reference docRef = new Reference();
            // Set identifier to the document's identifier
            if (sourceDocument.hasIdentifier()) {
            docRef.setIdentifier(sourceDocument.getIdentifier());
            }
            // Set display to the Composition.title if available
            if (sourceDocument.hasEntry() && sourceDocument.getEntryFirstRep().getResource() instanceof Composition) {
            Composition comp = (Composition) sourceDocument.getEntryFirstRep().getResource();
            if (comp.hasTitle()) {
                docRef.setDisplay(comp.getTitle());
            }
            }
            // Set type to "Bundle"
            docRef.setType("Bundle");

            entity.setWhat(docRef);
            provenance.addEntity(entity);
        }
    }

    /**
     * Finds the fullUrl of a resource in the given Bundle that matches the provided resource
     * by type and identifier. Supports Organization, Patient, RelatedPerson, Device, and Practitioner.
     *
     * @param resource The FHIR resource to match (Organization, Patient, RelatedPerson, Device, Practitioner).
     * @param bundle The Bundle to search within.
     * @return The fullUrl of the matching resource in the bundle, or null if not found.
     */
    private String findResourceFullUrlByIdentifier(IBaseResource resource, Bundle bundle) {
        if (resource == null || bundle == null) return null;

        List<Identifier> identifiers = null;
        String resourceType = resource.fhirType();

        // Extract identifiers based on resource type
        switch (resourceType) {
            case "Organization":
                identifiers = ((Organization) resource).getIdentifier();
                break;
            case "Patient":
                identifiers = ((Patient) resource).getIdentifier();
                break;
            case "RelatedPerson":
                identifiers = ((RelatedPerson) resource).getIdentifier();
                break;
            case "Device":
                identifiers = ((Device) resource).getIdentifier();
                break;
            case "Practitioner":
                identifiers = ((Practitioner) resource).getIdentifier();
                break;
            default:
                return null;
        }

        if (identifiers == null || identifiers.isEmpty()) return null;

        for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
            IBaseResource entryResource = entry.getResource();
            if (entryResource == null) continue;
            if (!resourceType.equals(entryResource.fhirType())) continue;

            List<Identifier> entryIdentifiers = null;
            switch (resourceType) {
                case "Organization":
                    entryIdentifiers = ((Organization) entryResource).getIdentifier();
                    break;
                case "Patient":
                    entryIdentifiers = ((Patient) entryResource).getIdentifier();
                    break;
                case "RelatedPerson":
                    entryIdentifiers = ((RelatedPerson) entryResource).getIdentifier();
                    break;
                case "Device":
                    entryIdentifiers = ((Device) entryResource).getIdentifier();
                    break;
                case "Practitioner":
                    entryIdentifiers = ((Practitioner) entryResource).getIdentifier();
                    break;
            }
            if (entryIdentifiers == null || entryIdentifiers.isEmpty()) continue;

            // Compare identifiers
            for (Identifier id1 : identifiers) {
                if (id1 == null || !id1.hasSystem() || !id1.hasValue()) continue;
                for (Identifier id2 : entryIdentifiers) {
                    if (id2 == null || !id2.hasSystem() || !id2.hasValue()) continue;
                    if (id1.getSystem().equals(id2.getSystem()) && id1.getValue().equals(id2.getValue())) {
                        return entry.getFullUrl();
                    }
                }
            }
        }
        return null;
    }

    /**
     * Creates a SectionComponent for the Medications section.
     * This is a placeholder method and should be implemented to retrieve actual medication data.
     */
    private SectionComponent problemsSection(List<Bundle> patientDocuments, Bundle mhrPsBundle, String mhrPsPatientUuid) {
        SectionComponent section = new SectionComponent();
        Narrative sectionNarrative = new Narrative();
        sectionNarrative.setStatus(Narrative.NarrativeStatus.GENERATED);

        section.setTitle("Problems List");
        section.setCode(new CodeableConcept().addCoding(
            new Coding()
                .setSystem("http://loinc.org")
                .setCode("11450-4")
        ));

        boolean hasProblems = false;
        if (patientDocuments != null) {
            for (Bundle document : patientDocuments) {
                if (document.hasIdentifier() && excludedSystem.equals(document.getIdentifier().getSystem())) continue;
                for (Bundle.BundleEntryComponent entry : document.getEntry()) {
                    IBaseResource resource = entry.getResource();
                    if (resource instanceof Condition) {
                        hasProblems = true;
                        Condition condition = (Condition) resource;
                        String conditionUuid = UUID.randomUUID().toString();

                        // Update the patient reference to the MHR PS Patient UUID
                        if (mhrPsPatientUuid != null) {
                            condition.setSubject(new Reference("urn:uuid:" + mhrPsPatientUuid));
                        }

                        Reference conditionReference = new Reference("urn:uuid:" + conditionUuid);
                        section.addEntry(conditionReference);

                        // Add the Condition resource to the MHR PS Bundle
                        Bundle.BundleEntryComponent conditionEntry = mhrPsBundle.addEntry();
                        conditionEntry.setFullUrl("urn:uuid:" + conditionUuid);
                        condition.setId(conditionUuid);
                        conditionEntry.setResource(condition);

                        // --- Add Provenance resource for this Condition ---
                        generateProvenance(mhrPsBundle, document, condition);

                        // Build up the table rows for each condition
                        String existingDiv = sectionNarrative.hasDiv() ? sectionNarrative.getDivAsString() : null;
                        StringBuilder tableRows = new StringBuilder();

                        // Extract fields
                        String conditionText = condition.getCode() != null ? condition.getCode().getText() : "";
                        String clinicalStatus = condition.hasClinicalStatus() && condition.getClinicalStatus().hasCoding()
                                ? condition.getClinicalStatus().getCodingFirstRep().getCode() : "";
                        String verificationStatus = condition.hasVerificationStatus() && condition.getVerificationStatus().hasCoding()
                                ? condition.getVerificationStatus().getCodingFirstRep().getCode() : "";
                        String onset = condition.hasOnset() ? condition.getOnset().toString() : "";

                        // Build the row
                        tableRows.append("<tr>")
                                .append("<td>").append(conditionText).append("</td>")
                                .append("<td>").append(clinicalStatus).append("</td>")
                                .append("<td>").append(verificationStatus).append("</td>")
                                .append("<td>").append(onset).append("</td>")
                                .append("</tr>");

                        // If this is the first condition, start the table
                        if (existingDiv == null || existingDiv.isEmpty()) {
                            String tableHeader = "<div xmlns=\"http://www.w3.org/1999/xhtml\">" +
                                    "<table border=\"1\"><thead><tr>" +
                                    "<th>Condition</th><th>Clinical Status</th><th>Verification Status</th><th>Onset</th>" +
                                    "</tr></thead><tbody>";
                            sectionNarrative.setDivAsString(tableHeader + tableRows.toString() + "</tbody></table></div>");
                        } else {
                            // Insert the new row before the closing tags
                            int insertPos = existingDiv.lastIndexOf("</tbody>");
                            if (insertPos != -1) {
                                String newDiv = existingDiv.substring(0, insertPos)
                                        + tableRows.toString()
                                        + existingDiv.substring(insertPos);
                                sectionNarrative.setDivAsString(newDiv);
                            }
                        }
                    }
                }
            }
        }

        if (!hasProblems) {
            section.setEmptyReason(new CodeableConcept()
                .addCoding(new Coding()
                    .setSystem("http://terminology.hl7.org/CodeSystem/list-empty-reason")
                    .setCode("unavailable"))
                .setText("No information available."));
            sectionNarrative.setDivAsString("<div xmlns=\"http://www.w3.org/1999/xhtml\">No problems or conditions recorded.</div>");
        }

        section.setText(sectionNarrative);
        return section;
    }

    /**
     * Creates a SectionComponent for the Medications section.
     * This is a placeholder method and should be implemented to retrieve actual medication data.
     */
    private SectionComponent medicationsSection(List<Bundle> patientDocuments, Bundle mhrPsBundle, String mhrPsPatientUuid) {
        SectionComponent section = new SectionComponent();
        Narrative sectionNarrative = new Narrative();
        sectionNarrative.setStatus(Narrative.NarrativeStatus.GENERATED);

        section.setTitle("Medication History");
        section.setCode(new CodeableConcept().addCoding(
            new Coding()
                .setSystem("http://loinc.org")
                .setCode("10160-0")
        ));

        boolean hasMedications = false;
        StringBuilder tableRows = new StringBuilder();
        String tableHeader = "<div xmlns=\"http://www.w3.org/1999/xhtml\">" +
                "<table border=\"1\"><thead><tr>" +
                "<th>Type</th><th>Medication</th><th>Status</th><th>Effective/Date</th><th>Dosage</th>" +
                "</tr></thead><tbody>";

        if (patientDocuments != null) {
            for (Bundle document : patientDocuments) {
                if (document.hasIdentifier() && excludedSystem.equals(document.getIdentifier().getSystem())) continue;
                for (Bundle.BundleEntryComponent entry : document.getEntry()) {
                    IBaseResource resource = entry.getResource();
                    String medType = null;
                    String medDisplay = "";
                    String status = "";
                    String effective = "";
                    String dosage = "";
                    Medication medicationResource = null;

                    // MedicationStatement
                    if (resource instanceof MedicationStatement) {
                        hasMedications = true;
                        medType = "MedicationStatement";
                        MedicationStatement ms = (MedicationStatement) resource;
                        String msUuid = UUID.randomUUID().toString();
                        Reference msReference = new Reference("urn:uuid:" + msUuid);
                        section.addEntry(msReference);
                        Bundle.BundleEntryComponent msEntry = mhrPsBundle.addEntry();
                        msEntry.setFullUrl("urn:uuid:" + msUuid);

                        // Update the patient reference to the MHR PS Patient UUID
                        if (mhrPsPatientUuid != null) {
                            ms.setSubject(new Reference("urn:uuid:" + mhrPsPatientUuid));
                        }

                        ms.setId(msUuid);
                        msEntry.setResource(ms);

                        // --- Add Provenance resource for this MedicationStatement ---
                        generateProvenance(mhrPsBundle, document, ms);

                        // Medication reference or code
                        if (ms.hasMedicationReference()) {
                            Reference medRef = ms.getMedicationReference();
                            medDisplay = medRef.getDisplay();
                            // Try to find Medication resource in the bundle
                            medicationResource = findAndAddMedicationResource(medRef, document, mhrPsBundle);
                        } else if (ms.hasMedicationCodeableConcept()) {
                            medDisplay = ms.getMedicationCodeableConcept().getText();
                        }
                        status = ms.hasStatus() ? ms.getStatus().toCode() : "";
                        effective = ms.hasEffective() ? ms.getEffective().toString() : "";
                        if (ms.hasDosage() && !ms.getDosage().isEmpty()) {
                            dosage = ms.getDosageFirstRep().getText();
                        }
                    }

                    // MedicationRequest
                    if (resource instanceof MedicationRequest) {
                        hasMedications = true;
                        medType = "MedicationRequest";
                        MedicationRequest mr = (MedicationRequest) resource;
                        String mrUuid = UUID.randomUUID().toString();
                        Reference mrReference = new Reference("urn:uuid:" + mrUuid);
                        section.addEntry(mrReference);
                        Bundle.BundleEntryComponent mrEntry = mhrPsBundle.addEntry();
                        mrEntry.setFullUrl("urn:uuid:" + mrUuid);

                        // Update the patient reference to the MHR PS Patient UUID
                        if (mhrPsPatientUuid != null) {
                            mr.setSubject(new Reference("urn:uuid:" + mhrPsPatientUuid));
                        }

                        mr.setId(mrUuid);
                        mrEntry.setResource(mr);

                        // --- Add Provenance resource for this MedicationRequest ---
                        generateProvenance(mhrPsBundle, document, mr);

                        if (mr.hasMedicationReference()) {
                            Reference medRef = mr.getMedicationReference();
                            medDisplay = medRef.getDisplay();
                            medicationResource = findAndAddMedicationResource(medRef, document, mhrPsBundle);
                        } else if (mr.hasMedicationCodeableConcept()) {
                            medDisplay = mr.getMedicationCodeableConcept().getText();
                        }
                        status = mr.hasStatus() ? mr.getStatus().toCode() : "";
                        effective = mr.hasAuthoredOn() ? mr.getAuthoredOn().toString() : "";
                        if (mr.hasDosageInstruction() && !mr.getDosageInstruction().isEmpty()) {
                            dosage = mr.getDosageInstructionFirstRep().getText();
                        }
                    }

                    // MedicationDispense
                    if (resource instanceof MedicationDispense) {
                        hasMedications = true;
                        medType = "MedicationDispense";
                        MedicationDispense md = (MedicationDispense) resource;
                        String mdUuid = UUID.randomUUID().toString();

                        // Update the patient reference to the MHR PS Patient UUID
                        if (mhrPsPatientUuid != null) {
                            md.setSubject(new Reference("urn:uuid:" + mhrPsPatientUuid));
                        }

                        Reference mdReference = new Reference("urn:uuid:" + mdUuid);
                        section.addEntry(mdReference);
                        Bundle.BundleEntryComponent mdEntry = mhrPsBundle.addEntry();
                        mdEntry.setFullUrl("urn:uuid:" + mdUuid);
                        md.setId(mdUuid);
                        mdEntry.setResource(md);
                        
                        // --- Add Provenance resource for this MedicationDispense ---
                        generateProvenance(mhrPsBundle, document, md);

                        if (md.hasMedicationReference()) {
                            Reference medRef = md.getMedicationReference();
                            medDisplay = medRef.getDisplay();
                            medicationResource = findAndAddMedicationResource(medRef, document, mhrPsBundle);
                        } else if (md.hasMedicationCodeableConcept()) {
                            medDisplay = md.getMedicationCodeableConcept().getText();
                        }
                        status = md.hasStatus() ? md.getStatus().toCode() : "";
                        effective = md.hasWhenHandedOver() ? md.getWhenHandedOver().toString() : "";
                        if (md.hasDosageInstruction() && !md.getDosageInstruction().isEmpty()) {
                            dosage = md.getDosageInstructionFirstRep().getText();
                        }
                    }

                    // MedicationAdministration
                    if (resource instanceof MedicationAdministration) {
                        hasMedications = true;
                        medType = "MedicationAdministration";
                        MedicationAdministration ma = (MedicationAdministration) resource;
                        String maUuid = UUID.randomUUID().toString();

                        // Update the patient reference to the MHR PS Patient UUID
                        if (mhrPsPatientUuid != null) {
                            ma.setSubject(new Reference("urn:uuid:" + mhrPsPatientUuid));
                        }

                        Reference maReference = new Reference("urn:uuid:" + maUuid);
                        section.addEntry(maReference);
                        Bundle.BundleEntryComponent maEntry = mhrPsBundle.addEntry();
                        maEntry.setFullUrl("urn:uuid:" + maUuid);
                        ma.setId(maUuid);
                        maEntry.setResource(ma);

                        // --- Add Provenance resource for this MedicationAdministration ---
                        generateProvenance(mhrPsBundle, document, ma);

                        if (ma.hasMedicationReference()) {
                            Reference medRef = ma.getMedicationReference();
                            medDisplay = medRef.getDisplay();
                            medicationResource = findAndAddMedicationResource(medRef, document, mhrPsBundle);
                        } else if (ma.hasMedicationCodeableConcept()) {
                            medDisplay = ma.getMedicationCodeableConcept().getText();
                        }
                        status = ma.hasStatus() ? ma.getStatus().toCode() : "";
                        effective = ma.hasEffective() ? ma.getEffective().toString() : "";
                        if (ma.hasDosage() && ma.getDosage().hasText()) {
                            dosage = ma.getDosage().getText();
                        }
                    }

                    // If a medication resource was found and not already in the bundle, add it
                    // (handled by findAndAddMedicationResource)

                    // Only add row if this entry was a medication resource
                    if (medType != null) {
                        tableRows.append("<tr>")
                                .append("<td>").append(medType).append("</td>")
                                .append("<td>").append(medDisplay != null ? medDisplay : "").append("</td>")
                                .append("<td>").append(status).append("</td>")
                                .append("<td>").append(effective).append("</td>")
                                .append("<td>").append(dosage).append("</td>")
                                .append("</tr>");
                    }
                }
            }
        }

        if (hasMedications) {
            sectionNarrative.setDivAsString(tableHeader + tableRows.toString() + "</tbody></table></div>");
        } else {
            section.setEmptyReason(new CodeableConcept()
                .addCoding(new Coding()
                    .setSystem("http://terminology.hl7.org/CodeSystem/list-empty-reason")
                    .setCode("unavailable"))
                .setText("No information available."));
            sectionNarrative.setDivAsString("<div xmlns=\"http://www.w3.org/1999/xhtml\">No medications recorded.</div>");
        }

        section.setText(sectionNarrative);
        return section;
    }

    /**
     * Helper to find a Medication resource by reference in the given document bundle,
     * and add it to the MHR PS bundle if not already present.
     */
    private Medication findAndAddMedicationResource(Reference medRef, Bundle document, Bundle mhrPsBundle) {
        if (medRef == null || medRef.getReference() == null) return null;
        String ref = medRef.getReference();
        // Only handle local references (e.g., "Medication/123" or "#med1")
        String id = null;
        if (ref.startsWith("Medication/")) {
            id = ref.substring("Medication/".length());
        } else if (ref.startsWith("#")) {
            id = ref.substring(1);
        }
        if (id == null) return null;

        // Search for Medication resource in the document bundle
        for (Bundle.BundleEntryComponent entry : document.getEntry()) {
            IBaseResource resource = entry.getResource();
            if (resource instanceof Medication) {
                Medication med = (Medication) resource;
                if (id.equals(med.getIdElement().getIdPart())) {
                    // Add to MHR PS bundle if not already present
                    boolean alreadyPresent = false;
                    for (Bundle.BundleEntryComponent mhrEntry : mhrPsBundle.getEntry()) {
                        if (mhrEntry.getResource() instanceof Medication) {
                            Medication mhrMed = (Medication) mhrEntry.getResource();
                            if (mhrMed.getIdElement().getIdPart().equals(med.getIdElement().getIdPart())) {
                                alreadyPresent = true;
                                break;
                            }
                        }
                    }
                    if (!alreadyPresent) {
                        String medUuid = UUID.randomUUID().toString();
                        med.setId(medUuid);
                        Bundle.BundleEntryComponent medEntry = mhrPsBundle.addEntry();
                        medEntry.setFullUrl("urn:uuid:" + medUuid);
                        medEntry.setResource(med);
                    }
                    return med;
                }
            }
        }
        return null;
    }


    /**
     * Creates a SectionComponent for the Immunizations section.
     * This section aggregates Immunization resources from the patient's documents.
     */
    private SectionComponent immunizationsSection(List<Bundle> patientDocuments, Bundle mhrPsBundle, String mhrPsPatientUuid, Date lookbackDate) {
        SectionComponent section = new SectionComponent();
        Narrative sectionNarrative = new Narrative();
        sectionNarrative.setStatus(Narrative.NarrativeStatus.GENERATED);

        section.setTitle("Immunizations History");
        section.setCode(new CodeableConcept().addCoding(
            new Coding()
                .setSystem("http://loinc.org")
                .setCode("11369-6")
        ));

        boolean hasImmunizations = false;
        StringBuilder tableRows = new StringBuilder();
        String tableHeader = "<div xmlns=\"http://www.w3.org/1999/xhtml\">" +
                "<table border=\"1\"><thead><tr>" +
                "<th>Vaccine Code</th><th>Occurrence Date</th>" +
                "</tr></thead><tbody>";

        if (patientDocuments != null) {
            for (Bundle document : patientDocuments) {
                if (document.hasIdentifier() && excludedSystem.equals(document.getIdentifier().getSystem())) continue;
                // Iterate through each entry in the document Bundle
                for (Bundle.BundleEntryComponent entry : document.getEntry()) {
                    IBaseResource resource = entry.getResource();
                    if (resource instanceof Immunization) {
                        Immunization immunization = (Immunization) resource;
                        // Only include if occurrenceDateTime is after lookbackDate (if lookbackDate is not null)
                        if (lookbackDate != null && immunization.hasOccurrenceDateTimeType()) {
                            Date occurrence = immunization.getOccurrenceDateTimeType().getValue();
                            if (occurrence == null || occurrence.before(lookbackDate)) {
                                continue;
                            }
                        }
                        hasImmunizations = true;
                        String immunizationUuid = UUID.randomUUID().toString();

                        // Update the patient reference to the MHR PS Patient UUID
                        if (mhrPsPatientUuid != null) {
                            immunization.setPatient(new Reference("urn:uuid:" + mhrPsPatientUuid));
                        }

                        Reference immunizationReference = new Reference("urn:uuid:" + immunizationUuid);
                        section.addEntry(immunizationReference);

                        // Add the Immunization resource to the MHR PS Bundle
                        Bundle.BundleEntryComponent immunizationEntry = mhrPsBundle.addEntry();
                        immunizationEntry.setFullUrl("urn:uuid:" + immunizationUuid);
                        immunization.setId(immunizationUuid);
                        immunizationEntry.setResource(immunization);

                        // --- Add Provenance resource for this Immunization ---
                        generateProvenance(mhrPsBundle, document, immunization);

                        // Build table row for this immunization
                        String vaccineCode = "";
                        if (immunization.hasVaccineCode() && immunization.getVaccineCode().hasCoding()) {
                            Coding coding = immunization.getVaccineCode().getCodingFirstRep();
                            vaccineCode = (coding.getSystem() != null ? coding.getSystem() + "|" : "") +
                                    (coding.getCode() != null ? coding.getCode() : "");
                        } else if (immunization.hasVaccineCode() && immunization.getVaccineCode().hasText()) {
                            vaccineCode = immunization.getVaccineCode().getText();
                        }
                        String occurrenceDate = "";
                        if (immunization.hasOccurrenceDateTimeType()) {
                            Date occurrence = immunization.getOccurrenceDateTimeType().getValue();
                            if (occurrence != null) {
                                occurrenceDate = new SimpleDateFormat("yyyy-MM-dd").format(occurrence);
                            }
                        }
                        tableRows.append("<tr>")
                                .append("<td>").append(vaccineCode).append("</td>")
                                .append("<td>").append(occurrenceDate).append("</td>")
                                .append("</tr>");
                    }
                }
            }
        }

        if (!hasImmunizations) {
            section.setEmptyReason(new CodeableConcept()
                .addCoding(new Coding()
                    .setSystem("http://terminology.hl7.org/CodeSystem/list-empty-reason")
                    .setCode("unavailable"))
                .setText("No information available."));
            sectionNarrative.setDivAsString("<div xmlns=\"http://www.w3.org/1999/xhtml\">No immunizations recorded.</div>");
        } else {
            sectionNarrative.setDivAsString(tableHeader + tableRows.toString() + "</tbody></table></div>");
        }

        section.setText(sectionNarrative);
        return section;
    }


    private SectionComponent procedureHistorySection(List<Bundle> patientDocuments, Bundle mhrPsBundle,
            String patientUuid, Date lookbackDate) {
        SectionComponent section = new SectionComponent();
        Narrative sectionNarrative = new Narrative();
        sectionNarrative.setStatus(Narrative.NarrativeStatus.GENERATED);
        section.setTitle("Procedure History");
        section.setCode(new CodeableConcept().addCoding(
            new Coding()
                .setSystem("http://loinc.org")
                .setCode("47519-4")
        ));
        boolean hasProcedures = false;
        StringBuilder tableRows = new StringBuilder();
        String tableHeader = "<div xmlns=\"http://www.w3.org/1999/xhtml\">" +
                "<table border=\"1\"><thead><tr>" +
                "<th>Procedure Code</th><th>Performed Date</th><th>Status</th>" +
                "</tr></thead><tbody>";
        if (patientDocuments != null) {
            for (Bundle document : patientDocuments) {
                if (document.hasIdentifier() && excludedSystem.equals(document.getIdentifier().getSystem())) continue;
                // Iterate through each entry in the document Bundle
                for (Bundle.BundleEntryComponent entry : document.getEntry()) {
                    IBaseResource resource = entry.getResource();
                    if (resource instanceof Procedure) {
                        Procedure procedure = (Procedure) resource;
                        // Only include if performedDateTime is after lookbackDate (if lookbackDate is not null)
                        if (lookbackDate != null && procedure.hasPerformedDateTimeType()) {
                            Date performed = procedure.getPerformedDateTimeType().getValue();
                            if (performed == null || performed.before(lookbackDate)) {
                                continue;
                            }
                        }
                        hasProcedures = true;
                        String procedureUuid = UUID.randomUUID().toString();

                        // Update the patient reference to the MHR PS Patient UUID
                        if (patientUuid != null) {
                            procedure.setSubject(new Reference("urn:uuid:" + patientUuid));
                        }

                        // update the performer of the procedure
                        // check if the performed is in the MhrPsBundle if yes, update the reference
                        // if not, add the performer to the MhrPsBundle
                        if (procedure.hasPerformer()) {
                            for (Procedure.ProcedurePerformerComponent performer : procedure.getPerformer()) {
                                if (performer.hasActor() && performer.getActor().getReference() != null) {
                                    String actorRef = performer.getActor().getReference();
                                    String actorFullUrl = null;
                                    IBaseResource actorResource = null;
                                    // Try to find the performer resource in the MHR PS Bundle
                                    for (Bundle.BundleEntryComponent sourceDocEntry : document.getEntry()) {
                                        if (sourceDocEntry.getFullUrl().equals(actorRef)) {
                                            actorResource = sourceDocEntry.getResource();
                                            actorFullUrl = findResourceFullUrlByIdentifier(actorResource, mhrPsBundle);
                                            break;
                                        }
                                    }
                                    if (actorFullUrl != null) {
                                        performer.setActor(new Reference(actorFullUrl));
                                    } else {
                                        Bundle.BundleEntryComponent newEntry = mhrPsBundle.addEntry();
                                        newEntry.setFullUrl(actorRef);
                                        newEntry.setResource((Resource) actorResource);
                                        //performer.setActor(new Reference(actorRef));
                                        
                                    }
                                }
                            }
                        }

                        Reference procedureReference = new Reference("urn:uuid:" + procedureUuid);
                        section.addEntry(procedureReference);

                        // Add the Procedure resource to the MHR PS Bundle
                        Bundle.BundleEntryComponent procedureEntry = mhrPsBundle.addEntry();
                        procedureEntry.setFullUrl("urn:uuid:" + procedureUuid);
                        procedure.setId(procedureUuid);
                        procedureEntry.setResource(procedure);

                        // --- Add Provenance resource for this Procedure ---
                        generateProvenance(mhrPsBundle, document, procedure);

                        // Build table row for this procedure
                        String codeText = "";
                        if (procedure.hasCode() && procedure.getCode().hasCoding()) {
                            Coding coding = procedure.getCode().getCodingFirstRep();
                            codeText = (coding.getSystem() != null ? coding.getSystem() + "|" : "") +
                                    (coding.getCode() != null ? coding.getCode() : "");
                        } else if (procedure.hasCode() && procedure.getCode().hasText()) {
                            codeText = procedure.getCode().getText();
                        }
                        String performedDate = "";
                        if (procedure.hasPerformedDateTimeType()) {
                            Date performed = procedure.getPerformedDateTimeType().getValue();
                            if (performed != null) {
                                performedDate = new SimpleDateFormat("yyyy-MM-dd").format(performed);
                            }
                        }
                        String status = procedure.hasStatus() ? procedure.getStatus().toCode() : "";
                        tableRows.append("<tr>")
                                .append("<td>").append(codeText).append("</td>")
                                .append("<td>").append(performedDate).append("</td>")
                                .append("<td>").append(status).append("</td>")
                                .append("</tr>");
                    }
                }
            }
        }

        if (!hasProcedures) {
            section.setEmptyReason(new CodeableConcept()
                .addCoding(new Coding()
                    .setSystem("http://terminology.hl7.org/CodeSystem/list-empty-reason")
                    .setCode("unavailable"))
                .setText("No information available."));
            sectionNarrative.setDivAsString("<div xmlns=\"http://www.w3.org/1999/xhtml\">No procedures recorded.</div>");
        } else {
            sectionNarrative.setDivAsString(tableHeader + tableRows.toString() + "</tbody></table></div>");
        }

        section.setText(sectionNarrative);
        return section;
    }


    /**
     * Creates a SectionComponent for the Patient Story section.
     * This section aggregates Goals and Composition narratives from the patient's documents.
     */
    private SectionComponent patientStorySection(List<Bundle> patientDocuments, Bundle mhrPsBundle, String mhrPsPatientUuid) {
        SectionComponent section = new SectionComponent();
        Narrative sectionNarrative = new Narrative();
        sectionNarrative.setStatus(Narrative.NarrativeStatus.ADDITIONAL);
        String sectionDiv = "";
        boolean hasPatientStory = false;

        section.setTitle("Patient Story");
        section.setCode(new CodeableConcept().addCoding(
            new Coding()
                .setSystem("http://loinc.org")
                .setCode("81338-6")
        ));

        // loop over the patient documents and add any Goal resources to the section
        if (patientDocuments != null) {
            for (Bundle document : patientDocuments) {
                // Iterate through each entry in the document Bundle
                for (Bundle.BundleEntryComponent entry : document.getEntry()) {
                    IBaseResource resource = entry.getResource();
                    if (resource instanceof Goal) {
                        Goal goal = (Goal) resource;
                        String goalUuid = UUID.randomUUID().toString();

                        // Update the patient reference to the MHR PS Patient UUID
                        if (mhrPsPatientUuid != null) {
                            goal.setSubject(new Reference("urn:uuid:" + mhrPsPatientUuid));
                            goal.setExpressedBy(new Reference("urn:uuid:" + mhrPsPatientUuid));
                        }

                        Reference goalReference = new Reference("urn:uuid:" + goalUuid);
                        section.addEntry(goalReference);

                        // Add the Goal resource to the MHR PS Bundle
                        Bundle.BundleEntryComponent goalEntry = mhrPsBundle.addEntry();
                        goalEntry.setFullUrl("urn:uuid:" + goalUuid);
                        goal.setId(goalUuid);
                        goalEntry.setResource(goal);

                        // --- Add Provenance resource for this Goal ---
                        generateProvenance(mhrPsBundle, document, goal);
                    }
                    // else if Resource is Composition, extract narrative text from Patient Story section and append to sectionDiv
                    else if (resource instanceof Composition) {
                        Composition composition = (Composition) resource;
                        // Find the section with code http://loinc.org|81338-6 and append its text to sectionDiv
                        if (composition.hasSection()) {
                            for (SectionComponent sec : composition.getSection()) {
                                if (sec.hasCode() && sec.getCode().hasCoding()) {
                                    for (Coding coding : sec.getCode().getCoding()) {
                                        if ("http://loinc.org".equals(coding.getSystem()) && "81338-6".equals(coding.getCode())) {
                                            if (sec.hasText() && sec.getText().hasDiv()) {
                                                String narrativeText = sec.getText().getDivAsString();
                                                if (narrativeText != null && !narrativeText.isEmpty()) {
                                                    sectionDiv += narrativeText;
                                                    hasPatientStory = true;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }


        // If no Goals or Composition narratives found, set empty reason
        if (!hasPatientStory) {
            section.setEmptyReason(new CodeableConcept()
                .addCoding(new Coding()
                    .setSystem("http://terminology.hl7.org/CodeSystem/list-empty-reason")
                    .setCode("unavailable"))
                .setText("No information available."));
            sectionDiv = "<div xmlns=\"http://www.w3.org/1999/xhtml\">No patient story recorded.</div>";
        }
        // Set the narrative text
        sectionNarrative.setDivAsString("<div xmlns=\"http://www.w3.org/1999/xhtml\">" + sectionDiv + "</div>");
        section.setText(sectionNarrative);

        return section;
    }

}
