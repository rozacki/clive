
use uc_clive;
set hive.cli.errors.ignore=true;


DROP VIEW IF EXISTS claimantCommitment_no_pii;
CREATE VIEW claimantCommitment_no_pii as SELECT claimantId, completedDateTime_d_date, contractId, createdDateTime_d_date, readAboutReducedPaymentsJournalId, toDoId, workGroup FROM claimantCommitment;

DROP VIEW IF EXISTS team_no_pii;
CREATE VIEW team_no_pii as SELECT createdDateTime_d_date, locationId, otherTeamTypeDescription, teamId, teamLeaderId, teamMemberIds, teamType FROM team;

DROP VIEW IF EXISTS personalDetails_no_pii;
CREATE VIEW personalDetails_no_pii as SELECT citizenId, contactPreference, contractId, createdDateTime_d_date, declarationId, effectiveDate_hasDate, gender, paymentEffectiveDate_hasDate, processId, sanitisedFirstName, sanitisedLastName, sanitisedMiddleName, verifiedUsingBioQuestionsOrThirdParty, verifiedWithNameDateOfBirthOrAddressChange, type FROM personalDetails;

DROP VIEW IF EXISTS claimantProfile_no_pii;
CREATE VIEW claimantProfile_no_pii as SELECT claimantProfileItems_createdByAgentId, claimantProfileItems_createdDateTime_d_date, claimantProfileItems_description, claimantProfileItems_id, claimantProfileItems_value, createdDateTime_d_date, claimantProfileId FROM claimantProfile;

DROP VIEW IF EXISTS healthDeclaration_no_pii;
CREATE VIEW healthDeclaration_no_pii as SELECT createdDateTime_d_date, declarationId, healthCircumstances_armedForcesIndependencePayment, healthCircumstances_attendanceAllowance, healthCircumstances_claimantId, healthCircumstances_constantAttendanceAllowance, healthCircumstances_contractId, healthCircumstances_declaredDateTime_d_date, healthCircumstances_dlaAllowance, healthCircumstances_effectiveDate_hasDate, healthCircumstances_healthCondition, healthCircumstances_paymentEffectiveDate_hasDate, healthCircumstances_piPayment, healthCircumstances_type, processId, type FROM healthDeclaration;

DROP VIEW IF EXISTS agent_no_pii;
CREATE VIEW agent_no_pii as SELECT agentId, createdDateTime_d_date, deliveryUnits, sortName FROM agent;

DROP VIEW IF EXISTS periodOfSickness_no_pii;
CREATE VIEW periodOfSickness_no_pii as SELECT claimantId, contractId, createdDateTime_d_date, endDate, fitNotes__version, fitNotes_consentToContactDoctor, fitNotes_createdDateTime_d_date, fitNotes_doctorOrMedicalCentre, fitNotes_doctorsDetails__version, fitNotes_doctorsDetails_address, fitNotes_doctorsDetails_createdDateTime_d_date, fitNotes_endDate, fitNotes_fitNoteId, fitNotes_startDate, fitNotes_evidenceAccepted, periodOfSicknessId, selfCertifications_createdDateTime_d_date, selfCertifications_startDate, startDate FROM periodOfSickness;

DROP VIEW IF EXISTS housingDeclaration_no_pii;
CREATE VIEW housingDeclaration_no_pii as SELECT contractId, councilTax_appliedToCouncilTaxReduction, councilTax_councilTaxInClaimantName, createdDateTime_d_date, declarationId, declaredDateTime_d_date, effectiveDate_hasDate, livingDetails, noApplicableHousingCostsDeclarationDetails_noApplicableHousingCostsType, otherHousingDeclarationDetails_furtherDetails, otherHousingDeclarationDetails_otherAccommodationType, ownPropertyDeclarationDetails_haveMortgage, ownPropertyDeclarationDetails_payServiceCharges, ownPropertyDeclarationDetails_mortgageDetailsList_amount, ownPropertyDeclarationDetails_mortgageDetailsList_forDisabilityAdaptation, ownPropertyDeclarationDetails_mortgageDetailsList_lender, ownPropertyDeclarationDetails_mortgageDetailsList_lenderCode, ownPropertyDeclarationDetails_mortgageDetailsList_loanReferenceNumber, ownPropertyDeclarationDetails_mortgageDetailsList_mortgageDetailsId, ownPropertyDeclarationDetails_serviceChargeDetails_amount, ownPropertyDeclarationDetails_serviceChargeDetails_frequency, paymentEffectiveDate_hasDate, privateRentedPropertyDeclarationDetails_additionalRentedHousingInformation_exemptFromTheSharedRoomRate, privateRentedPropertyDeclarationDetails_additionalRentedHousingInformation_nonCommercialRelationshipWithLandlord, privateRentedPropertyDeclarationDetails_additionalRentedHousingInformation_supportedExemptAccommodation, privateRentedPropertyDeclarationDetails_bedroomAndTenancy_bothYouAndPartnerNamedOnTenancyOrRentalAgreement, privateRentedPropertyDeclarationDetails_bedroomAndTenancy_jointTenancyWithSomeoneOtherThanPartner, privateRentedPropertyDeclarationDetails_bedroomAndTenancy_numberOfBedrooms, privateRentedPropertyDeclarationDetails_jointTenancy_numberOfTenants, privateRentedPropertyDeclarationDetails_jointTenancy_rent_amount, privateRentedPropertyDeclarationDetails_jointTenancy_rent_paymentFrequency, privateRentedPropertyDeclarationDetails_jointTenancy_rent_type, privateRentedPropertyDeclarationDetails_jointTenancy_serviceCharges_amount, privateRentedPropertyDeclarationDetails_jointTenancy_serviceCharges_paymentFrequency, privateRentedPropertyDeclarationDetails_jointTenancy_serviceCharges_type, privateRentedPropertyDeclarationDetails_landlordDetails_payRentTo, privateRentedPropertyDeclarationDetails_landlordRelationshipDetails_anyoneCloseRelative, privateRentedPropertyDeclarationDetails_landlordRelationshipDetails_anyoneFinancialInterest, privateRentedPropertyDeclarationDetails_landlordRelationshipDetails_liveAtSameProperty, privateRentedPropertyDeclarationDetails_rentDetails_regularPayment_amount, privateRentedPropertyDeclarationDetails_rentDetails_regularPayment_paymentFrequency, privateRentedPropertyDeclarationDetails_rentDetails_regularPayment_type, privateRentedPropertyDeclarationDetails_rentDetails_startDate, processId, socialAccommodationDeclarationDetails_additionalRentedHousingInformation_exemptFromTheSharedRoomRate, socialAccommodationDeclarationDetails_additionalRentedHousingInformation_nonCommercialRelationshipWithLandlord, socialAccommodationDeclarationDetails_additionalRentedHousingInformation_supportedExemptAccommodation, socialAccommodationDeclarationDetails_bedroomAndTenancy_bothYouAndPartnerNamedOnTenancyOrRentalAgreement, socialAccommodationDeclarationDetails_bedroomAndTenancy_jointTenancyWithSomeoneOtherThanPartner, socialAccommodationDeclarationDetails_bedroomAndTenancy_numberOfBedrooms, socialAccommodationDeclarationDetails_jointTenancy_numberOfTenants, socialAccommodationDeclarationDetails_jointTenancy_rent_amount, socialAccommodationDeclarationDetails_jointTenancy_rent_paymentFrequency, socialAccommodationDeclarationDetails_jointTenancy_rent_type, socialAccommodationDeclarationDetails_jointTenancy_serviceCharges_amount, socialAccommodationDeclarationDetails_jointTenancy_serviceCharges_paymentFrequency, socialAccommodationDeclarationDetails_jointTenancy_serviceCharges_type, socialAccommodationDeclarationDetails_landlordDetails_payRentTo, socialAccommodationDeclarationDetails_rentDetails_regularPayment_amount, socialAccommodationDeclarationDetails_rentDetails_regularPayment_paymentFrequency, socialAccommodationDeclarationDetails_rentDetails_regularPayment_type, socialAccommodationDeclarationDetails_rentDetails_startDate, socialAccommodationDeclarationDetails_socialHousing_rentFreeWeeks, socialAccommodationDeclarationDetails_socialHousing_serviceCharges_amount, socialAccommodationDeclarationDetails_socialHousing_serviceCharges_paymentFrequency, socialAccommodationDeclarationDetails_socialHousing_serviceCharges_type, type FROM housingDeclaration;

DROP VIEW IF EXISTS childcareDeclaration_no_pii;
CREATE VIEW childcareDeclaration_no_pii as SELECT contractId, createdDateTime_d_date, declarationId, declaredChildcareProviders_childcareCosts_amountPaid, declaredChildcareProviders_childcareCosts_children_ageInYears, declaredChildcareProviders_childcareCosts_children_childId, declaredChildcareProviders_childcareCosts_children_gender, declaredChildcareProviders_childcareCosts_dateDeclared, declaredChildcareProviders_childcareCosts_datePaid, declaredChildcareProviders_childcareCosts_dateVerified, declaredChildcareProviders_childcareCosts_id, declaredChildcareProviders_childcareCosts_periodEnd, declaredChildcareProviders_childcareCosts_periodStart, declaredChildcareProviders_childcareCosts_providerId, declaredChildcareProviders_childcareProvider_address1, declaredChildcareProviders_childcareProvider_address2, declaredChildcareProviders_childcareProvider_id, declaredChildcareProviders_childcareProvider_providerName, declaredChildcareProviders_childcareProvider_registrationNumber, declaredChildcareProviders_childcareProvider_town, effectiveDate_hasDate, hasChildcareCosts, paymentEffectiveDate_hasDate, processId, updatedChildcareCostIds, type FROM childcareDeclaration;

DROP VIEW IF EXISTS claimant_no_pii;
CREATE VIEW claimant_no_pii as SELECT anonymous, bookAppointmentStatus, caseManager, cisInterestStatus, citizenId, claimantDeathDetails_dateOfDeath, claimantDeathDetails_deathVerified, createdDateTime_d_date, currentWorkGroupOnCurrentContract, deliveryUnits, govUkVerifyStatus, hasVerifiedEmailId, identityDocumentsStatus, inactiveAccountEmailReminderSentDate_d_date, managedJourney_ableToViewJournal, personId, pilotGroup, supportingAgents, webAnalyticsUserId, searchableNames FROM claimant;

DROP VIEW IF EXISTS educationDeclaration_no_pii;
CREATE VIEW educationDeclaration_no_pii as SELECT createdDateTime_d_date, declarationId, educationCircumstances_claimantId, educationCircumstances_contractId, educationCircumstances_declaredDateTime_d_date, educationCircumstances_effectiveDate_hasDate, educationCircumstances_fullTimeEducation, educationCircumstances_paymentEffectiveDate_hasDate, educationCircumstances_type, processId, type FROM educationDeclaration;

DROP VIEW IF EXISTS todo_no_pii;
CREATE VIEW todo_no_pii as SELECT attachment_fileName, attachment_fileSize, attachment_id, attachment_revision, attachment_updatedAt_d_date, attachment_updatedBy, canDo, canSee, cancelled, completion_completionDate, completion_completionTime, completion_version, contractId, createdDateTime_d_date, deadlineDate, deadlineTime, description, flags, informAgentWhenCompleted, relatedToClaimantId, status, title, toDoId, type FROM todo;

DROP VIEW IF EXISTS workGroupAllocation_no_pii;
CREATE VIEW workGroupAllocation_no_pii as SELECT citizenId, contractId, createdDateTime_d_date, currentWorkGroup, expectedWorkingHours, workGroupAllocationId FROM workGroupAllocation;

DROP VIEW IF EXISTS workAndEarningsDeclaration_no_pii;
CREATE VIEW workAndEarningsDeclaration_no_pii as SELECT childTaxCredit_dateStopped, childTaxCredit_receivedInLast5Weeks, citizenId, contractId, createdDateTime_d_date, declarationId, earnings_amount, earnings_hoursPerWeek, earnings_paymentFrequency, effectiveDate_hasDate, employerAndMaternityAllowance_maternityAllowance, employerAndMaternityAllowance_maternityAllowanceAmount, employerAndMaternityAllowance_maternityAllowancePaymentFrequency, employerAndMaternityAllowance_otherEmployerPaymentFrequency, employerAndMaternityAllowance_otherPayFromEmployer, employerAndMaternityAllowance_otherPayFromEmployerAmount, employmentStatus, expectedPreviousEarnings, futureEmployment_expectedEarnings, futureEmployment_hoursPerWeek, futureEmployment_paymentFrequency, futureEmployment_startDate, futureEmployment_startingEmployment, paymentEffectiveDate_hasDate, processId, startingSelfEmployment, workHistory_dateStoppedWorking, workHistory_stoppedWorkingInLast9Months, working, workingTaxCredit_dateStopped, workingTaxCredit_receivedInLast5Weeks, type FROM workAndEarningsDeclaration;

DROP VIEW IF EXISTS agent_todo_no_pii;
CREATE VIEW agent_todo_no_pii as SELECT allocatedToAgentId, alpForm_alpFormType, alpForm_attachmentSummary_fileSize, alpForm_attachmentSummary_revision, alpForm_attachmentSummary_updatedAt_d_date, alpForm_attachmentSummary_updatedBy, cancelled, completedDateTime_d_date, completingAgentId, contractId, createdByAgentId, createdByAgentName, createdDateTime_d_date, deadlineDate, deadlineTime, deliveryUnits, notes_createdBy, notes_createdByName, notes_dateCreated_d_date, notes_deleted, relatedToClaimantId, status, supportingDocuments_attachmentSummary_fileSize, supportingDocuments_attachmentSummary_revision, supportingDocuments_attachmentSummary_updatedAt_d_date, supportingDocuments_attachmentSummary_updatedBy, supportingDocuments_supportingDocumentType, toDoId, type FROM agent_todo;

DROP VIEW IF EXISTS childDeclaration_no_pii;
CREATE VIEW childDeclaration_no_pii as SELECT children_adoptedOrUnderGuardianship, children_dateOfBirth, children_declarationDate, children_disabilityDetails_dlaCareComponent, children_disabilityDetails_pipDailyLivingComponent, children_disabilityDetails_receivingDisabilityLivingAllowance, children_disabilityDetails_receivingPersonalIndependencePayment, children_disabilityDetails_registeredBlind, children_gender, children_id, children_inQualifyingFullTimeEducation, children_nonDependantDetails_awayOnArmedForcesOperations, children_nonDependantDetails_personIsClaimantsChild, children_nonDependantDetails_primaryCarer, children_nonDependantDetails_receivingArmedForcesIndependencePayment, children_nonDependantDetails_receivingAttendanceAllowance, children_nonDependantDetails_receivingCarersAllowance, children_nonDependantDetails_receivingDisabilityLivingAllowance, children_nonDependantDetails_receivingPersonalIndependencePayment, children_nonDependantDetails_receivingStatePensionCredit, children_removalDate, children_temporaryAbsence_abroadForMoreThanOneMonth, children_temporaryAbsence_inPrison, children_temporaryAbsence_inTheCareOfLocalAuthority, contractId, createdDateTime_d_date, declarationId, effectiveDate_hasDate, hasExceptionToTwoChildPolicy, paymentEffectiveDate_hasDate, primaryCarerId, processId, type FROM childDeclaration;

DROP VIEW IF EXISTS systemWorkGroupAllocation_no_pii;
CREATE VIEW systemWorkGroupAllocation_no_pii as SELECT calculatedWorkGroup, claimantId, contractId, createdDateTime_d_date, effectiveDate, expectedWorkingHours, superseded, systemWorkGroupAllocationId FROM systemWorkGroupAllocation;

DROP VIEW IF EXISTS contract_no_pii;
CREATE VIEW contract_no_pii as SELECT annualVerificationCompletionDate, annualVerificationEligibilityDate, assessmentPeriods_assessmentPeriodId, assessmentPeriods_contractId, assessmentPeriods_createdDateTime_d_date, assessmentPeriods_endDate, assessmentPeriods_paymentDate, assessmentPeriods_processDate, assessmentPeriods_startDate, claimClosureReason, claimSuspension_reasonsForSuspension, claimSuspension_suspensionDate, claimantsExemptFromWaitingDays, closeProcessStartDate_d_date, closedDate, contractId, contractType, coupleContract, createdDateTime_d_date, declaredDate, entitlementDate, flags, paymentDayOfMonth, people, startDate FROM contract;

DROP VIEW IF EXISTS manualOverride_no_pii;
CREATE VIEW manualOverride_no_pii as SELECT contractId, createdDateTime_d_date, forType FROM manualOverride;

DROP VIEW IF EXISTS healthAndDisabilityDeclaration_no_pii;
CREATE VIEW healthAndDisabilityDeclaration_no_pii as SELECT abilityToWork_hasAFitNote, abilityToWork_restrictedAbilityToWork, agentToCallTerminallyIllClaimant, claimantId, contractId, copiedFromDeclarationId, createdDateTime_d_date, declarationId, effectiveDate_hasDate, hospitalStays_admissionDate, hospitalStays_dischargeDate, hospitalStays_hospitalStay, medicalTreatments_startDate, medicalTreatments_treatment, paymentEffectiveDate_hasDate, periodOfSicknessId, pregnancy_dueDate, pregnancy_illnessPutsPregnancyAtRisk, processId, reportingIllness, supportAtWork_hasGuideDog, supportAtWork_hasWheelchair, supportAtWork_needsHearingLoop, supportAtWork_otherSupport, terminalIllness_prognosis, terminalIllness_terminallyIll, type FROM healthAndDisabilityDeclaration;

DROP VIEW IF EXISTS claimant_commitment_no_pii;
CREATE VIEW claimant_commitment_no_pii as SELECT claimantId, completedDateTime_d_date, contractId, createdDateTime_d_date, properties_claimantCommitmentVersion, readAboutReducedPaymentsJournalId, toDoId, workGroup FROM claimant_commitment;

DROP VIEW IF EXISTS agentWorkGroupAllocation_no_pii;
CREATE VIEW agentWorkGroupAllocation_no_pii as SELECT agentWorkGroupAllocationId, claimantId, contractId, createdDateTime_d_date, effectiveDate, endDate, endReason, overriddenWorkGroup, overrideReason, removed, removedReason FROM agentWorkGroupAllocation;

DROP VIEW IF EXISTS journal_no_pii;
CREATE VIEW journal_no_pii as SELECT agentFirstName, agentLastName, agentLocation, agentResponse, agentResponseDateTime_d_date, alertAdviser, attachment_fileName, attachment_fileSize, attachment_id, attachment_revision, attachment_updatedAt_d_date, attachment_updatedBy, claimantCanReply, claimantId, contractId, createdBy, createdByAdviser, createdDateTime_d_date, deleted_d_date, journalEntryCategory, journalEntryProperties_type, journalEntryThreadId, journalId, messageType, title, type FROM journal;

DROP VIEW IF EXISTS agent_todo_archive_no_pii;
CREATE VIEW agent_todo_archive_no_pii as SELECT allocatedToAgentId, alpForm_alpFormType, alpForm_attachmentSummary_fileSize, alpForm_attachmentSummary_revision, alpForm_attachmentSummary_updatedAt_d_date, alpForm_attachmentSummary_updatedBy, cancelled, completedDateTime_d_date, completingAgentId, contractId, createdByAgentId, createdByAgentName, createdDateTime_d_date, deadlineDate, deadlineTime, deliveryUnits, notes_createdBy, notes_createdByName, notes_dateCreated_d_date, notes_deleted, relatedToClaimantId, status, supportingDocuments_attachmentSummary_fileSize, supportingDocuments_attachmentSummary_revision, supportingDocuments_attachmentSummary_updatedAt_d_date, supportingDocuments_attachmentSummary_updatedBy, supportingDocuments_supportingDocumentType, toDoId, type FROM agent_todo_archive;

DROP VIEW IF EXISTS carerDeclaration_no_pii;
CREATE VIEW carerDeclaration_no_pii as SELECT carerCircumstances_careeCircumstances_dateOfBirth, carerCircumstances_careeCircumstances_relationship, carerCircumstances_caringCircumstances_caringForSomeone, carerCircumstances_caringCircumstances_personReceivesBenefit, carerCircumstances_claimantId, carerCircumstances_contractId, carerCircumstances_declaredDateTime_d_date, carerCircumstances_effectiveDate_hasDate, carerCircumstances_moreThan35Hours, carerCircumstances_otherCaringResponsibilities_caringForSomeoneElse, carerCircumstances_otherCaringResponsibilities_personReceivesBenefit, carerCircumstances_paymentEffectiveDate_hasDate, carerCircumstances_stoppedCaringCircumstances_dueToDeath, carerCircumstances_stoppedCaringCircumstances_stoppedDate, carerCircumstances_timeOffCircumstances_endDate, carerCircumstances_timeOffCircumstances_startDate, carerCircumstances_type, createdDateTime_d_date, declarationId, processId, type FROM carerDeclaration;

