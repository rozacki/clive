// Author Chris Rozacki
// what are collections primary keys
//todo: add database
var collection_ids = {
    "agentToDo":{"path":"_id.toDoId"}
    ,"agentToDoArchive":{"path":"_id.toDoId"}
    ,"journal":{"path":"_id.journalId"}
    ,"toDo":{"path":"_id.toDoId"}
    ,"contract":{"path":"_id.contractId"}
    ,"address":{"path":"_id.d_oid"}
    ,"apa":{"path":"_id.d_oid"}
    ,"bankDetails":{"path":"_id.d_oid"}
    ,'alternativePaymentArrangement':{"path":"_id.declarationId"}
    ,'apa'							:{"path":"_id.declarationId"}
    ,'bankDetails'					:{"path":"_id.declarationId"}
    ,'calculateDeductions'			:{"path":"_id.assessmentPeriodId"}
    ,'capital'						:{"path":"_id.declarationId"}
    ,'carerCircumstances'			:{"path":"_id.d_oid"}
    ,'carersAllowance'				:{"path":"_id.d_oid"}
    ,'childElementEligibility'		:{"path":"_id.d_oid"}
    ,'childcare'					:{"path":"_id.d_oid"}
    ,'childrenCircumstances'		:{"path":"_id.declarationId"}
    ,'earningsData'             	:{"path":"_id.d_oid"}
    ,'educationCircumstances'		:{"path":"_id.d_oid"}
    ,'employmentCircumstances'		:{"path":"_id.d_oid"}
    ,'gracePeriod'					:{"path":"_id.d_oid"}
    ,'guardiansAllowance'			:{"path":"_id.d_oid"}
    ,'healthAndDisabilityCircumstances'	:{"path":"_id.d_oid"}
    ,'healthCircumstances'				:{"path":"_id.d_oid"}
    ,'housingCircumstances'				:{"path":"_id.d_oid"}
    ,'housingEntitlement'				:{"path":"_id.d_oid"}
    ,'ineligiblePartner'				:{"path":"_id.d_oid"}
    ,'nationality'						:{"path":"_id.d_oid"}
    ,'otherBenefit'						:{"path":"_id.d_oid"}
    ,'otherIncome'						:{"path":"_id.d_oid"}
    ,'overlappingBenefit'				:{"path":"_id.d_oid"}
    ,'partnerQuestion'					:{"path":"_id.d_oid"}
    ,'personDetails'					:{"path":"_id.d_oid"}
    ,'pregnancy'						:{"path":"_id.d_oid"}
    ,'previousEarnings'					:{"path":"_id.d_oid"}
    ,'supportForMortgageInterest'		:{"path":"_id.d_oid"}
    ,'terminalIllness'					:{"path":"_id.d_oid"}
    ,'workAndEarningsCircumstances'		:{"path":"_id.d_oid"}
    ,'workCapabilityAssessmentDecision'	:{"path":"_id.d_oid"}
    ,'carerDeclaration'                :{"path":"_id.declarationId"}
    ,'childcareDeclaration'            :{"path":"_id.declarationId"}
    ,'childDeclaration'                :{"path":"_id.declarationId"}
    ,'claimant'                        :{"path":"_id.citizenId"}
    ,'claimantCommitment'               :[{"path":"_id.contractId",'name':'contractId'},{"path":"_id.claimantId",'name':'claimantId'}]
    ,'educationDeclaration'             :{"path":"_id.declarationId"}
    ,'healthAndDisabilityDeclaration'   :{"path":"_id.declarationId"}
    ,'healthDeclaration'                :{"path":"_id.declarationId"}
    ,'housingDeclaration'               :{"path":"_id.declarationId"}
    ,'periodOfSickness'                 :{"path":"_id.periodOfSicknessId"}
    ,'personalDetails'                  :{"path":"_id.declarationId"}
    ,'statement'                        :{"path":"_id.statementId"}
    ,'survey'                           :{"path":"_id.d_oid"}
    ,'workAndEarningsDeclaration'       :{"path":"_id.declarationId"}
    ,'workGroupAllocation'              :{"path":"_id.d_oid"}
    ,'agent'                            :{'path':'_id.agentId'}
    ,'agentWorkGroupAllocation'         :{'path':'_id.agentWorkGroupAllocationId'}
    ,'claimantProfile'                  :{'path':'_id.claimantId'}
    ,'systemWorkGroupAllocation'        :{'path':'_id.systemWorkGroupAllocationId'}
    ,'team'                             :{'path':'_id.teamId'}
    ,'fraudPenalty'                     :{'path':'_id.fraudPenaltyId'}
    ,'outOfClaimDays'                   :{'path':'_id.personId'}
    ,'sanctionEscalation'               :{'path':'_id.sanctionId'}
    ,'sanctionProgress'                 :{'path':'_id.sanctionId'}
    ,'advanceDebtPosition'              :{'path':'_id.advanceGroupId'}
    ,'advanceGroup'                     :{'path':'_id.advanceGroupId'}
    ,'recoverableHardshipPayment'       :{'path':'_id.recoverableHardshipPaymentId'}
    ,'localUserMatchingData'            :{'path':'_id.personId'}
    ,'managementInformation'            :{'path':'_id.managementInformationId'}
    ,'matchingServiceRequests'          :{'path':'_id.matchingServiceRequestId'}
    ,'appointment'                      :{'path':'_id.appointmentId'}
    ,'deliveryUnitAddress'              :{'path':'_id.addressId'}
    ,'manualOverride'                   :{'path':'_id.contractId'}
    ,'sanction'                         :{'path':'_id.sanctionId'}
    ,'sanctionTerminationDecision'      :{'path':'_id.sanctionTerminationDecisionId'}
    ,'youthObligationDetails'           :{'path':'_id.youthObligationDetailsId'}
    ,'claimantIdentityVerification'     :{'path':'_id.claimantIdentityVerificationId'}
    ,'proveYourIdentityTrial'           :{'path':'_id.contractId'}
    ,'debtPosition'                      :{'path':'_id.debtPositionId'}
    ,'debtInterest'                      :{'path':'_id.claimantId'}

    // Villani project
    ,"interactions"                     :{"path": "_id"}
    ,"contacts"                         :{"path": "_id"}
    ,"devices"                          :{"path": "_id"}
}