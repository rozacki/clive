
use uc_clive;
set hive.cli.errors.ignore=true;
drop table IF EXISTS agent_todo_REVIEW_CLAIMANT_TODO;
drop table IF EXISTS agent_todo_archive_EARNINGS_SUPPLIED;
drop table IF EXISTS claimantCommitment;
drop table IF EXISTS agent_todo_MAKE_AN_ADVANCE_PAYMENT;
drop table IF EXISTS journal_legacyFraudPenaltyRemovedJournalEntry;
drop table IF EXISTS agent_todo_STOPPED_CARING;
drop table IF EXISTS agent_todo_archive_CHECK_WORKGROUP_AND_COMMITMENT_ON_HEALTH_AND_DISABILITY_CHANGE;
drop table IF EXISTS agent_todo_REFER_OVERPAYMENT;
drop table IF EXISTS todo_CHILDCARE_COSTS_CHANGE_CIRCS;
drop table IF EXISTS appointment;
drop table IF EXISTS youthObligationDetails;
drop table IF EXISTS journal_WordyJournalEntry;
drop table IF EXISTS todo_PROVIDE_PROOF_OF_YOUR_CHILDCARE_COSTS;
drop table IF EXISTS agent_todo_OFFER_WCA_REFERRAL_PROPERTIES;
drop table IF EXISTS agent_todo_APA_CREATE;
drop table IF EXISTS claimantIdentityVerification;
drop table IF EXISTS proveYourIdentityTrial;
drop table IF EXISTS agent_todo_VERIFY_SOCIAL_HOUSING_PROPERTIES;
drop table IF EXISTS team;
drop table IF EXISTS journal_SubmitClaimJournalEntry;
drop table IF EXISTS journal_FlexDirectPaymentToLandlordCancelledJournalEntry;
drop table IF EXISTS journal_CarerStoppedSummaryJournalEntry;
drop table IF EXISTS personalDetails;
drop table IF EXISTS agent_todo_MAKE_A_DECISION_ALP_TO_DO_PROPERTIES;
drop table IF EXISTS agent_todo_archive_MAKE_A_DECISION_LATE_REPORTING_OF_CHANGE_TO_DO_PROPERTIES;
drop table IF EXISTS agent_todo_archive_UNMATCHED_CLAIMANT_TO_DO_PROPERTIES;
drop table IF EXISTS journal_GenericJournalEntry;
drop table IF EXISTS agent_todo_PERMITTED_PERIOD_EXPIRING_SOON;
drop table IF EXISTS agent_todo_archive_REPORT_THIS_SELF_EMPLOYMENT;
drop table IF EXISTS otherIncome;
drop table IF EXISTS todo_PREPARE_TO_MEET_WORK_COACH_TODO_PROPERTIES;
drop table IF EXISTS agent_todo_UPDATE_CIS_AFTER_CHANGE_OF_CIRCS;
drop table IF EXISTS agent_todo_CALL_TERMINALLY_ILL_PROPERTIES;
drop table IF EXISTS capital;
drop table IF EXISTS journal_TakingPartInANewServicePilotJournalEntry;
drop table IF EXISTS agent_todo_APA_CHECK_LANDLORD;
drop table IF EXISTS claimantProfile;
drop table IF EXISTS agent_todo_WCA_DECISION;
drop table IF EXISTS journal_BenefitCapNotificationJournalEntry;
drop table IF EXISTS earningsData_SELF_EMPLOYED_EARNINGS_WITH_LATEST_MIF_DATA;
drop table IF EXISTS address;
drop table IF EXISTS agent_todo_archive_REVIEW_DEADLINE_TODO;
drop table IF EXISTS agent_todo_REFER_ENTITLEMENT_TO_DECISION_MAKER_PROPERTIES;
drop table IF EXISTS agent_todo_REPORT_THIS;
drop table IF EXISTS agent_todo_REVIEW_CLAIMANT_UPLOAD_PROPERTIES;
drop table IF EXISTS survey;
drop table IF EXISTS earningsData_SELF_EMPLOYED;
drop table IF EXISTS fraudPenalty;
drop table IF EXISTS todo_CHILDCARE_COSTS_PROVIDE_PROOF;
drop table IF EXISTS journal_ucOpenEndedSanctionAddedJournalEntry;
drop table IF EXISTS educationCircumstances;
drop table IF EXISTS journal_WhoLivesWithYouSummaryJournalEntry;
drop table IF EXISTS agent_todo_SCOTTISH_FLEX_DIRECT_PAYMENT_TO_LANDLORD;
drop table IF EXISTS sanction;
drop table IF EXISTS journal_HousingCostsChangeSummaryJournalEntry;
drop table IF EXISTS apa;
drop table IF EXISTS agent_todo_CONSIDER_WCA_REFERRAL_PROPERTIES;
drop table IF EXISTS claimant_commitment_HIGH-V2;
drop table IF EXISTS agent_todo_archive_RTE_TO_DO_PROPERTIES;
drop table IF EXISTS journal_legacySanctionRemovedJournalEntry;
drop table IF EXISTS agent_todo_archive_CALL_TERMINALLY_ILL_PROPERTIES;
drop table IF EXISTS agent_todo_UPDATE_CIS_PROPERTIES;
drop table IF EXISTS agent_todo_REJECTED_CARING;
drop table IF EXISTS journal_PaymentStoppedJournalEntry;
drop table IF EXISTS todo_READ_ABOUT_REDUCED_PAYMENTS_TODO_PROPERTIES;
drop table IF EXISTS agent_todo_archive_REFER_OVERPAYMENT;
drop table IF EXISTS sanctionEscalation;
drop table IF EXISTS recoverableHardshipPayment;
drop table IF EXISTS agent_todo_NEW_RELATIONSHIP;
drop table IF EXISTS agent_todo_archive_ACCOUNT_LOCKED_PROPERTIES;
drop table IF EXISTS agent_todo_archive_RELATIONSHIP_ENDED;
drop table IF EXISTS agent_todo_ASSESSMENT_PERIOD;
drop table IF EXISTS terminalIllness;
drop table IF EXISTS healthDeclaration;
drop table IF EXISTS agent;
drop table IF EXISTS todo_REPORT_SELF_EMPLOYMENT_EARNINGS_FEEDBACK_PROPERTIES;
drop table IF EXISTS agent_todo_archive_ENDING_SELF_EMPLOYMENT;
drop table IF EXISTS agent_todo_archive_UPDATE_CIS_FROM_CIS_500_PROPERTIES;
drop table IF EXISTS agent_todo_archive_MAKE_A_PAYMENT;
drop table IF EXISTS periodOfSickness;
drop table IF EXISTS agent_todo_archive_PREPARE_COMMITMENTS_TO_DO_PROPERTIES;
drop table IF EXISTS todo_SOCIAL_HOUSING_AGREE_CHANGES_TO_DO_PROPERTIES;
drop table IF EXISTS journal_ProvideFurtherProofOfYourChildcareCostsJournalEntry;
drop table IF EXISTS housingDeclaration;
drop table IF EXISTS guardiansAllowance;
drop table IF EXISTS agent_todo_MAKE_A_DECISION_LATE_REPORTING_OF_CHANGE_TO_DO_PROPERTIES;
drop table IF EXISTS journal_HaveALookTodoJournalEntry;
drop table IF EXISTS agent_todo_archive_CONFIRM_BENEFIT_DETAILS;
drop table IF EXISTS workAndEarningsCircumstances;
drop table IF EXISTS agent_todo_CHECK_PERSONAL_DETAILS;
drop table IF EXISTS ineligiblePartner;
drop table IF EXISTS todo_ATTEND_PHONE_APPOINTMENT_TODO_PROPERTIES;
drop table IF EXISTS agent_todo_AGENT_GENERATED_TO_DO_PROPERTIES;
drop table IF EXISTS journal_BankDetailsSummaryJournalEntry;
drop table IF EXISTS childcareDeclaration;
drop table IF EXISTS agent_todo_SELF_EMPLOYED_EARNINGS_TO_DO_PROPERTIES;
drop table IF EXISTS agent_todo_archive_REVIEW_SANCTIONS_TODO;
drop table IF EXISTS claimant;
drop table IF EXISTS agent_todo_RELATIONSHIP_ENDED;
drop table IF EXISTS agent_todo_archive_END_OF_RELATIONSHIP_CONTACT_CLAIMANT;
drop table IF EXISTS agent_todo_archive_INVESTIGATE_CIS_INTEREST_ERROR;
drop table IF EXISTS todo_ATTEND_IN_PERSON_APPOINTMENT_TODO_PROPERTIES;
drop table IF EXISTS journal_FebruaryPaymentJournalEntry;
drop table IF EXISTS educationDeclaration;
drop table IF EXISTS bankDetails;
drop table IF EXISTS agent_todo_EARNINGS_SUPPLIED;
drop table IF EXISTS agent_todo_HOUSING_DECLARATION_TO_DO_PROPERTIES;
drop table IF EXISTS agent_todo_STARTING_SELF_EMPLOYMENT;
drop table IF EXISTS journal_HousingSummaryJournalEntry;
drop table IF EXISTS agent_todo_CHECK_WORKGROUP_AND_COMMITMENTS_FOR_PREGNANCY;
drop table IF EXISTS agent_todo_ADDRESS_SUMMARY;
drop table IF EXISTS agent_todo_archive_BACKDATED_CLAIM;
drop table IF EXISTS agent_todo_archive_INFORM_CLAIMANT_LENDER_END_SMI_TODO_PROPERTIES;
drop table IF EXISTS agent_todo_archive_UPDATE_CIS_AFTER_CHANGE_OF_CIRCS;
drop table IF EXISTS agent_todo_archive_MAKE_A_DECISION_ALP_TO_DO_PROPERTIES;
drop table IF EXISTS journal_AddressSummaryJournalEntry;
drop table IF EXISTS agent_todo_archive_INTERVENTION_REVIEW;
drop table IF EXISTS todo;
drop table IF EXISTS journal_ClaimantNotificationJournalEntry;
drop table IF EXISTS workGroupAllocation;
drop table IF EXISTS housingCircumstances;
drop table IF EXISTS employmentCircumstances;
drop table IF EXISTS earningsData_SELF_REPORTED;
drop table IF EXISTS journal_ucSanctionAddedJournalEntry;
drop table IF EXISTS agent_todo_archive_TIME_OFF_CARING;
drop table IF EXISTS agent_todo_DATA_GATHER_REFER_HRT_TO_DECISION_MAKER;
drop table IF EXISTS agent_todo_archive_VERIFY_FILE_UPLOADED_CHILDCARE;
drop table IF EXISTS agent_todo_WHO_LIVES_WITH_YOU_SUMMARY;
drop table IF EXISTS agent_todo_BOOK_APPOINTMENT_WORK_GROUP_CHANGE;
drop table IF EXISTS agent_todo_CONSIDER_CLOSING_CLAIM;
drop table IF EXISTS agent_todo_BACKDATED_CLAIM;
drop table IF EXISTS debtPosition;
drop table IF EXISTS todo_CLAIMANT_NOTES;
drop table IF EXISTS todo_ACCEPT_COMMITMENTS_TODO_PROPERTIES;
drop table IF EXISTS journal_ChildcareSummaryJournalEntry;
drop table IF EXISTS workAndEarningsDeclaration;
drop table IF EXISTS agent_todo_archive_EXTRA_BEDROOM_DUE_TO_DISABILITY;
drop table IF EXISTS agent_todo;
drop table IF EXISTS todo_ANNUAL_VERIFICATION_TODO_PROPERTIES;
drop table IF EXISTS agent_todo_archive_CALCULATE_DEDUCTIONS_PROPERTIES;
drop table IF EXISTS agent_todo_CALCULATE_DEDUCTIONS_PROPERTIES;
drop table IF EXISTS agent_todo_RECORD_IN_WORK_PROGRESSION;
drop table IF EXISTS statement;
drop table IF EXISTS sanctionProgress;
drop table IF EXISTS journal_CarerTimeOffSummaryJournalEntry;
drop table IF EXISTS agent_todo_archive_REFER_ENTITLEMENT_TO_DECISION_MAKER_PROPERTIES;
drop table IF EXISTS agent_todo_CHECK_CIS_FOR_CARERS_ALLOWANCE_TO_DO_PROPERTIES;
drop table IF EXISTS agent_todo_archive_REPORT_THIS;
drop table IF EXISTS agent_todo_VERIFY_SELF_EMPLOYMENT;
drop table IF EXISTS agent_todo_archive_PERMITTED_PERIOD_EXPIRING_SOON;
drop table IF EXISTS agent_todo_archive_HRT_ENTITLEMENT_DECISION;
drop table IF EXISTS todo_UPLOAD_DOCUMENTS_TODO;
drop table IF EXISTS agent_todo_PERSON_SUMMARY;
drop table IF EXISTS agent_todo_ENDING_SELF_EMPLOYMENT;
drop table IF EXISTS agent_todo_RECOVER_OVERLAPPING_BENEFIT_PAYMENTS;
drop table IF EXISTS agent_todo_archive_INTERVENTION_FEEDBACK;
drop table IF EXISTS journal_undefined;
drop table IF EXISTS managementInformation;
drop table IF EXISTS agent_todo_CALCULATE_PAYMENT_PROPERTIES;
drop table IF EXISTS agent_todo_UPDATE_CIS_FROM_CIS_500_PROPERTIES;
drop table IF EXISTS agent_todo_archive_PERSON_SUMMARY;
drop table IF EXISTS childcare;
drop table IF EXISTS workCapabilityAssessmentDecision;
drop table IF EXISTS personDetails;
drop table IF EXISTS carersAllowance;
drop table IF EXISTS journal_ProvideProofOfYourChildcareProviderJournalEntry;
drop table IF EXISTS agent_todo_archive_OVERDUE_TO_DOS_FOR_CALCULATION;
drop table IF EXISTS advanceDebtPosition;
drop table IF EXISTS agent_todo_archive_VERIFY_PROPERTIES;
drop table IF EXISTS agent_todo_ABSENT_CHILD;
drop table IF EXISTS agent_todo_RETROSPECTIVE_CALC_PROPERTIES;
drop table IF EXISTS journal_ucFraudPenaltyRemovedJournalEntry;
drop table IF EXISTS agent_todo_archive_RTE_SUPPLIED;
drop table IF EXISTS claimant_commitment_LOW-V2;
drop table IF EXISTS childDeclaration;
drop table IF EXISTS agent_todo_HRT_ENTITLEMENT_DECISION;
drop table IF EXISTS agent_todo_GATHER_EVIDENCE_FOR_WAITING_DAYS;
drop table IF EXISTS agent_todo_WORK_ALLOWANCE_CHANGES_CONTACT_CLAIMANT;
drop table IF EXISTS todo_PROVIDE_FURTHER_PROOF_OF_YOUR_CHILDCARE_COSTS;
drop table IF EXISTS agent_todo_archive_STARTED_CARING;
drop table IF EXISTS todo_REPORT_SELF_EMPLOYED_EARNINGS_PROPERTIES;
drop table IF EXISTS agent_todo_CHECK_PREGNANCY_DETAILS_PROPERTIES;
drop table IF EXISTS agent_todo_archive_REJECTED_CARING;
drop table IF EXISTS agent_todo_HRT_ENTITLEMENT_RECHECK_PROPERTIES;
drop table IF EXISTS agent_todo_archive_ASSESSMENT_PERIOD;
drop table IF EXISTS todo_INITIAL_GATHER;
drop table IF EXISTS healthAndDisabilityCircumstances;
drop table IF EXISTS journal_WaitingDaysJournalEntry;
drop table IF EXISTS debtInterest;
drop table IF EXISTS carerCircumstances;
drop table IF EXISTS agent_todo_GENERATE_STATEMENT;
drop table IF EXISTS agent_todo_UNMATCHED_CLAIMANT_TO_DO_PROPERTIES;
drop table IF EXISTS todo_REPORT_SELF_EMPLOYMENT_EARNINGS_PROPERTIES;
drop table IF EXISTS todo_REDECLARE_CIRCUMSTANCES;
drop table IF EXISTS agent_todo_EARNINGS;
drop table IF EXISTS agent_todo_archive_ADD_CHILD;
drop table IF EXISTS agent_todo_archive_JOURNAL_ENTRY;
drop table IF EXISTS todo_HAVE_A_LOOK_TODO_PROPERTIES;
drop table IF EXISTS agent_todo_archive_MAKE_A_PAYMENT_TODO_PROPERTIES;
drop table IF EXISTS agent_todo_archive_WORK_AND_EARNINGS_SUMMARY;
drop table IF EXISTS agent_todo_RTE_SUPPLIED;
drop table IF EXISTS systemWorkGroupAllocation;
drop table IF EXISTS agent_todo_INVESTIGATE_CIS_INTEREST_ERROR;
drop table IF EXISTS agent_todo_CONFIRM_BENEFIT_DETAILS;
drop table IF EXISTS agent_todo_archive_CONSIDER_CLOSING_CLAIM;
drop table IF EXISTS agent_todo_archive_UPDATE_CIS_PROPERTIES;
drop table IF EXISTS agent_todo_archive_AGENT_GENERATED_TO_DO_PROPERTIES;
drop table IF EXISTS agent_todo_archive_SELF_EMPLOYED_EARNINGS_TO_DO_PROPERTIES;
drop table IF EXISTS agent_todo_RTE_TO_DO_PROPERTIES;
drop table IF EXISTS childrenCircumstances;
drop table IF EXISTS agent_todo_archive_FOR_ASSESSMENT_PERIOD_PROPERTIES;
drop table IF EXISTS agent_todo_archive_ADDRESS_SUMMARY;
drop table IF EXISTS agent_todo_INTERVENTION_REVIEW;
drop table IF EXISTS journal_ApaApprovedJournalEntry;
drop table IF EXISTS agent_todo_archive_RETROSPECTIVE_CALC_PROPERTIES;
drop table IF EXISTS agent_todo_UPDATE_CIS_FROM_TRACE_CIS_PROPERTIES;
drop table IF EXISTS agent_todo_CHECK_CALCULATION_PROPERTIES;
drop table IF EXISTS journal_ucFraudPenaltyAddedJournalEntry;
drop table IF EXISTS journal_legacyFraudPenaltyAddedJournalEntry;
drop table IF EXISTS journal_CompleteANewServicePilotJournalEntry;
drop table IF EXISTS agent_todo_INFORM_CLAIMANT_LENDER_END_SMI_TODO_PROPERTIES;
drop table IF EXISTS agent_todo_archive_CHECK_WORKGROUP_AND_COMMITMENTS_FOR_PREGNANCY;
drop table IF EXISTS nationality;
drop table IF EXISTS journal_ProvideProofOfYourChildcareCostsJournalEntry;
drop table IF EXISTS agent_todo_INVESTIGATE_DUPLICATE_ACCOUNT_TODO_PROPERTIES;
drop table IF EXISTS journal_advanceDeferredJournalEntry;
drop table IF EXISTS contract;
drop table IF EXISTS previousEarnings;
drop table IF EXISTS agent_todo_archive_DIFFERENCE_BETWEEN_CALCULATION_AND_PAYMENTS;
drop table IF EXISTS housingEntitlement;
drop table IF EXISTS agent_todo_archive_REVIEW_MINIMUM_INCOME_FLOOR_TO_DO_PROPERTIES;
drop table IF EXISTS agent_todo_archive_WORK_ALLOWANCE_CHANGES_CONTACT_CLAIMANT;
drop table IF EXISTS agent_todo_archive_REVIEW_CLAIMANT_TODO;
drop table IF EXISTS journal_ReviewUploadDocumentsJournalEntry;
drop table IF EXISTS claimant_commitment_MEDIUM-V2;
drop table IF EXISTS agent_todo_archive_NOTIFY_EXISTING_BENEFIT_TO_DO_PROPERTIES;
drop table IF EXISTS pregnancy;
drop table IF EXISTS agent_todo_archive_ENTITLEMENT_REVIEW_PROPERTIES;
drop table IF EXISTS agent_todo_archive_EARNINGS;
drop table IF EXISTS todo_BANK_DETAILS_CHANGE;
drop table IF EXISTS journal_COMPLETED_ATTEND_APPOINTMENT_JOURNAL_PROPERTIES;
drop table IF EXISTS agent_todo_INTERVENTION_FEEDBACK;
drop table IF EXISTS advanceGroup;
drop table IF EXISTS agent_todo_archive_VERIFY_SOCIAL_HOUSING_PROPERTIES;
drop table IF EXISTS agent_todo_CHECK_MEDICAL_EVIDENCE_PROPERTIES;
drop table IF EXISTS manualOverride;
drop table IF EXISTS childElementEligibility;
drop table IF EXISTS agent_todo_REVIEW_DEADLINE_TODO;
drop table IF EXISTS outOfClaimDays;
drop table IF EXISTS agent_todo_archive_CHECK_CALCULATION_PROPERTIES;
drop table IF EXISTS agent_todo_archive_UPDATE_CIS_FROM_TRACE_CIS_PROPERTIES;
drop table IF EXISTS agent_todo_MAKE_A_PAYMENT_TODO_PROPERTIES;
drop table IF EXISTS overlappingBenefit;
drop table IF EXISTS sanctionTerminationDecision;
drop table IF EXISTS claimant_commitment_NONE-V2;
drop table IF EXISTS agent_todo_END_OF_RELATIONSHIP_CONTACT_CLAIMANT;
drop table IF EXISTS agent_todo_archive_NON_DEPENDENT_HOUSING_COST_CONTRIBUTION;
drop table IF EXISTS journal_FlexDirectPaymentToLandlordClaimantToDoJournalEntry;
drop table IF EXISTS healthAndDisabilityDeclaration;
drop table IF EXISTS agent_todo_REVIEW_SANCTIONS_TODO;
drop table IF EXISTS agent_todo_PERSON_DETAILS_PROPERTIES;
drop table IF EXISTS claimant_commitment_MEDIUM;
drop table IF EXISTS agent_todo_REPORT_THIS_SELF_EMPLOYMENT;
drop table IF EXISTS agent_todo_archive_CHECK_PREGNANCY_DETAILS_PROPERTIES;
drop table IF EXISTS todo_UPLOAD_DOCUMENTS_TODO_CREATION;
drop table IF EXISTS agent_todo_archive_RECORD_OVERLAPPING_BENEFIT_TO_DO_PROPERTIES;
drop table IF EXISTS todo_PROVIDE_FIT_NOTE_REMINDER;
drop table IF EXISTS claimant_commitment;
drop table IF EXISTS agent_todo_archive_GENERATE_STATEMENT;
drop table IF EXISTS journal_WorkAndEarningsSummaryJournalEntry;
drop table IF EXISTS todo_STOPPED_WORKING_PREVIOUS_EARNINGS;
drop table IF EXISTS agent_todo_archive_MAKE_AN_AUTO_CALCULATED_PAYMENT;
drop table IF EXISTS agent_todo_archive_ABSENT_CHILD;
drop table IF EXISTS agent_todo_ADD_CHILD;
drop table IF EXISTS journal_ProvideAFitNoteSummaryJournalEntry;
drop table IF EXISTS journal_AdhocTodoJournalEntry;
drop table IF EXISTS agent_todo_archive_REVIEW_CLAIMANT_UPLOAD_PROPERTIES;
drop table IF EXISTS agent_todo_STARTED_CARING;
drop table IF EXISTS agent_todo_MAKE_A_PAYMENT;
drop table IF EXISTS agentWorkGroupAllocation;
drop table IF EXISTS supportForMortgageInterest;
drop table IF EXISTS agent_todo_RECORD_OVERLAPPING_BENEFIT_TO_DO_PROPERTIES;
drop table IF EXISTS agent_todo_archive_WHO_LIVES_WITH_YOU_SUMMARY;
drop table IF EXISTS agent_todo_archive_CHECK_MEDICAL_EVIDENCE_PROPERTIES;
drop table IF EXISTS agent_todo_archive_MAKE_AN_ADVANCE_PAYMENT;
drop table IF EXISTS claimant_commitment_NONE;
drop table IF EXISTS agent_todo_ACCOUNT_LOCKED_PROPERTIES;
drop table IF EXISTS agent_todo_NON_DEPENDENT_HOUSING_COST_CONTRIBUTION;
drop table IF EXISTS journal_AnnualVerificationJournalEntry;
drop table IF EXISTS agent_todo_FOR_ASSESSMENT_PERIOD_PROPERTIES;
drop table IF EXISTS agent_todo_DIFFERENCE_BETWEEN_CALCULATION_AND_PAYMENTS;
drop table IF EXISTS agent_todo_archive_NEW_RELATIONSHIP;
drop table IF EXISTS agent_todo_archive_RECOVER_OVERLAPPING_BENEFIT_PAYMENTS;
drop table IF EXISTS agent_todo_MAKE_AN_AUTO_CALCULATED_PAYMENT;
drop table IF EXISTS agent_todo_archive_APA_CHECK_LANDLORD;
drop table IF EXISTS claimant_commitment_HIGH;
drop table IF EXISTS localUserMatchingData;
drop table IF EXISTS agent_todo_VERIFY_CHILDCARE;
drop table IF EXISTS agent_todo_OVERDUE_TO_DOS_FOR_CALCULATION;
drop table IF EXISTS agent_todo_REVIEW_MINIMUM_INCOME_FLOOR_TO_DO_PROPERTIES;
drop table IF EXISTS agent_todo_archive_CHECK_PERSONAL_DETAILS;
drop table IF EXISTS agent_todo_archive_CHECK_CIS_FOR_CARERS_ALLOWANCE_TO_DO_PROPERTIES;
drop table IF EXISTS claimant_commitment_LOW;
drop table IF EXISTS agent_todo_archive_VERIFY_CHILDCARE;
drop table IF EXISTS agent_todo_TIME_OFF_CARING;
drop table IF EXISTS agent_todo_archive_CALCULATE_PAYMENT_PROPERTIES;
drop table IF EXISTS journal;
drop table IF EXISTS agent_todo_VERIFY_PROPERTIES;
drop table IF EXISTS calculateDeductions;
drop table IF EXISTS agent_todo_CHECK_WORKGROUP_AND_COMMITMENT_ON_HEALTH_AND_DISABILITY_CHANGE;
drop table IF EXISTS agent_todo_DATE_OF_BIRTH_SUMMARY;
drop table IF EXISTS todo_VERIFY_YOUR_IDENTITY_TODO_PROPERTIES;
drop table IF EXISTS agent_todo_archive_CONSIDER_WCA_REFERRAL_PROPERTIES;
drop table IF EXISTS partnerQuestion;
drop table IF EXISTS agent_todo_ENTITLEMENT_REVIEW_PROPERTIES;
drop table IF EXISTS agent_todo_PREPARE_COMMITMENTS_TO_DO_PROPERTIES;
drop table IF EXISTS todo_CONTACT_SERVICE_CENTRE_TODO_PROPERTIES;
drop table IF EXISTS journal_ucSanctionRemovedJournalEntry;
drop table IF EXISTS agent_todo_archive_VERIFY_SELF_EMPLOYMENT;
drop table IF EXISTS agent_todo_archive_HRT_ENTITLEMENT_RECHECK_PROPERTIES;
drop table IF EXISTS agent_todo_JOURNAL_ENTRY;
drop table IF EXISTS agent_todo_archive_APA_CREATE;
drop table IF EXISTS agent_todo_archive_STOPPED_CARING;
drop table IF EXISTS journal_PersonSummaryJournalEntry;
drop table IF EXISTS journal_FlexMoreFrequentPaymentsClaimantToDoJournalEntry;
drop table IF EXISTS agent_todo_archive_INVESTIGATE_DUPLICATE_ACCOUNT_TODO_PROPERTIES;
drop table IF EXISTS todo_CHILDCARE_PROVIDER_PROVIDE_PROOF;
drop table IF EXISTS journal_advanceApprovedJournalEntry;
drop table IF EXISTS agent_todo_archive_DATA_GATHER_REFER_HRT_TO_DECISION_MAKER;
drop table IF EXISTS todo_DROP_IN_EARNINGS_PREVIOUS_EARNINGS;
drop table IF EXISTS journal_FlexMoreFrequentPaymentsAgentToDoJournalEntry;
drop table IF EXISTS agent_todo_archive_DATE_OF_BIRTH_SUMMARY;
drop table IF EXISTS agent_todo_WORK_AND_EARNINGS_SUMMARY;
drop table IF EXISTS agent_todo_archive;
drop table IF EXISTS deliveryUnitAddress;
drop table IF EXISTS todo_ATTEND_APPOINTMENT_TODO_PROPERTIES;
drop table IF EXISTS agent_todo_VERIFY_FILE_UPLOADED_CHILDCARE;
drop table IF EXISTS agent_todo_archive_OFFER_WCA_REFERRAL_PROPERTIES;
drop table IF EXISTS agent_todo_archive_STARTING_SELF_EMPLOYMENT;
drop table IF EXISTS agent_todo_archive_BOOK_APPOINTMENT_WORK_GROUP_CHANGE;
drop table IF EXISTS earningsData_RTE_FEED_TO_DO;
drop table IF EXISTS journal_HealthAndDisabilitySummaryJournalEntry;
drop table IF EXISTS healthCircumstances;
drop table IF EXISTS agent_todo_archive_RECORD_IN_WORK_PROGRESSION;
drop table IF EXISTS journal_legacySanctionAddedJournalEntry;
drop table IF EXISTS agent_todo_archive_WCA_DECISION;
drop table IF EXISTS gracePeriod;
drop table IF EXISTS agent_todo_archive_PERSON_DETAILS_PROPERTIES;
drop table IF EXISTS earningsData_CALCULATED_EARNINGS;
drop table IF EXISTS agent_todo_archive_SCOTTISH_FLEX_DIRECT_PAYMENT_TO_LANDLORD;
drop table IF EXISTS otherBenefit;
drop table IF EXISTS agent_todo_NOTIFY_EXISTING_BENEFIT_TO_DO_PROPERTIES;
drop table IF EXISTS agent_todo_EXTRA_BEDROOM_DUE_TO_DISABILITY;
drop table IF EXISTS journal_FlexDirectPaymentToLandlordAgentToDoJournalEntry;
drop table IF EXISTS journal_CarerStartedSummaryJournalEntry;
drop table IF EXISTS agent_todo_archive_HOUSING_DECLARATION_TO_DO_PROPERTIES;
drop table IF EXISTS journal_FlexMoreFrequentPaymentsCancelledJournalEntry;
drop table IF EXISTS carerDeclaration;
drop table IF EXISTS agent_todo_archive_GATHER_EVIDENCE_FOR_WAITING_DAYS;
