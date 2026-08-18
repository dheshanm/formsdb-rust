/// Map RPMS event ID to REDCap event name using hashify
pub fn rpms_to_redcap_event(key: i32) -> Option<&'static str> {
    let key_str = key.to_string();
    hashify::tiny_map! {
        key_str.as_bytes(),
        "0" => "unknown",
        "1" => "screening",
        "2" => "baseline",
        "3" => "month_1",
        "4" => "month_2",
        "5" => "month_3",
        "6" => "month_4",
        "7" => "month_5",
        "8" => "month_6",
        "9" => "month_7",
        "10" => "month_8",
        "11" => "month_9",
        "12" => "month_10",
        "13" => "month_11",
        "14" => "month_12",
        "15" => "month_18",
        "16" => "month_24",
        "17" => "other_study",
        "22" => "other_study",
        "98" => "conversion",
        "99" => "floating_forms",
        "100" => "screening", // Self Consent
        "101" => "screening", // Parental Consent
    }
}

pub const VISIT_ORDER: &[&str] = &[
    "screening",
    "baseline",
    "month_1",
    "month_2",
    "month_3",
    "month_4",
    "month_5",
    "month_6",
    "month_7",
    "month_8",
    "month_9",
    "month_10",
    "month_11",
    "month_12",
    "month_18",
    "month_24",
];

pub const FORM_NAME_RPMS_SUFFIXES: &[(&str, &str)] = &[
    (
        "informed_consent_run_sheet",
        "informed_consent_run_sheet.csv",
    ),
    ("adverse_events", "adverse_events.csv.flat"),
    ("assist", "assist.csv"),
    (
        "blood_sample_preanalytic_quality_assurance",
        "blood_sample_preanalytic_quality_assurance.csv.flat",
    ),
    ("bprs", "bprs.csv"),
    ("cbc_with_differential", "cbc_with_differential.csv"),
    ("cdss", "cdss.csv"),
    ("coenrollment_form", "coenrollment_form.csv"),
    ("conversion_form", "conversion_form.csv"),
    ("cssrs_baseline", "cssrs_baseline.csv"),
    ("cssrs_followup", "cssrs_followup.csv"),
    ("current_health_status", "current_health_status.csv"),
    (
        "current_pharmaceutical_treatment_floating_med_125",
        "current_pharmaceutical_treatment_floating_med_125.csv.flat",
    ),
    (
        "current_pharmaceutical_treatment_floating_med_2650",
        "current_pharmaceutical_treatment_floating_med_2650.csv.flat",
    ),
    (
        "daily_activity_and_saliva_sample_collection",
        "daily_activity_and_saliva_sample_collection.csv",
    ),
    (
        "digital_biomarkers_axivity_checkin",
        "digital_biomarkers_axivity_checkin.csv",
    ),
    (
        "digital_biomarkers_axivity_end_of_12month__study_pe",
        "digital_biomarkers_axivity_end_of_12month_study_pe.csv",
    ),
    (
        "digital_biomarkers_axivity_onboarding",
        "digital_biomarkers_axivity_onboarding.csv",
    ),
    (
        "digital_biomarkers_mindlamp_checkin",
        "digital_biomarkers_mindlamp_checkin.csv",
    ),
    (
        "digital_biomarkers_mindlamp_end_of_12month__study_p",
        "digital_biomarkers_mindlamp_end_of_12month_study_p.csv",
    ),
    (
        "digital_biomarkers_mindlamp_onboarding",
        "digital_biomarkers_mindlamp_onboarding.csv",
    ),
    ("eeg_run_sheet", "eeg_run_sheet.csv"),
    (
        "family_interview_for_genetic_studies_figs",
        "family_interview_for_genetic_studies_figs.csv.flat",
    ),
    ("gcp_cbc_with_differential", "gcp_cbc_with_differential.csv"),
    ("gcp_current_health_status", "gcp_current_health_status.csv"),
    (
        "global_functioning_role_scale",
        "global_functioning_role_scale.csv",
    ),
    (
        "global_functioning_role_scale_followup",
        "global_functioning_role_scale_followup.csv",
    ),
    (
        "global_functioning_social_scale",
        "global_functioning_social_scale.csv",
    ),
    (
        "global_functioning_social_scale_followup",
        "global_functioning_social_scale_followup.csv",
    ),
    ("guid_form", "guid_form.csv"),
    (
        "health_conditions_genetics_fluid_biomarkers",
        "health_conditions_genetics_fluid_biomarkers.csv",
    ),
    (
        "health_conditions_medicalpsychiatric_history",
        "health_conditions_medical_historypsychiatric_histo.csv.flat",
    ),
    (
        "inclusionexclusion_criteria_review",
        "inclusionexclusion_criteria_review.csv",
    ),
    (
        "iq_assessment_wasiii_wiscv_waisiv",
        "iq_assessment_wasiii_wiscv_waisiv.csv",
    ),
    ("item_promis_for_sleep", "item_promis_for_sleep.csv"),
    (
        "lifetime_ap_exposure_screen",
        "lifetime_ap_exposure_screen.csv",
    ),
    ("missing_data", "missing_data.csv"),
    ("mri_run_sheet", "mri_run_sheet.csv"),
    ("nsipr", "nsipr.csv"),
    ("oasis", "oasis.csv"),
    (
        "past_pharmaceutical_treatment",
        "past_pharmaceutical_treatment.csv.flat",
    ),
    ("penncnb", "penncnb.csv"),
    (
        "perceived_discrimination_scale",
        "perceived_discrimination_scale.csv",
    ),
    ("perceived_stress_scale", "perceived_stress_scale.csv"),
    ("pgis", "pgis.csv"),
    (
        "premorbid_adjustment_scale",
        "premorbid_adjustment_scale.csv",
    ),
    (
        "premorbid_iq_reading_accuracy",
        "premorbid_iq_reading_accuracy.csv",
    ),
    ("psychosis_polyrisk_score", "psychosis_polyrisk_score.csv"),
    (
        "psychosocial_treatment_form",
        "psychosocial_treatment_form.csv.flat",
    ),
    (
        "psychs_av_recording_run_sheet",
        "psychs_av_recording_run_sheet.csv",
    ),
    ("psychs_p1p8", "psychs_p1p8.csv"),
    ("psychs_p1p8_fu", "psychs_p1p8_fu.csv"),
    ("psychs_p1p8_fu_hc", "psychs_p1p8_fu_hc.csv"),
    ("psychs_p9ac32", "psychs_p9ac32.csv"),
    ("psychs_p9ac32_fu", "psychs_p9ac32_fu.csv"),
    ("psychs_p9ac32_fu_hc", "psychs_p9ac32_fu_hc.csv"),
    (
        "pubertal_developmental_scale",
        "pubertal_developmental_scale.csv",
    ),
    ("ra_prediction", "ra_prediction.csv"),
    ("recruitment_source", "recruitment_source.csv"),
    ("resource_use_log", "resource_use_log.csv.flat"),
    (
        "scid5_psychosis_mood_substance_abuse",
        "scid5_psychosis_mood_substance_abuse.csv",
    ),
    (
        "scid5_schizotypal_personality_sciddpq",
        "scid5_schizotypal_personality_sciddpq.csv",
    ),
    ("sociodemographics", "sociodemographics.csv"),
    ("sofas_followup", "sofas_followup.csv"),
    ("sofas_screening", "sofas_screening.csv"),
    ("speech_sampling_run_sheet", "speech_sampling_run_sheet.csv"),
    (
        "traumatic_brain_injury_screen",
        "traumatic_brain_injury_screen.csv.flat",
    ),
];

pub const FORM_NAME_TO_ABBRV: &[(&str, &str)] = &[
    ("enrollment_note", "enrollment_note"),
    ("informed_consent_run_sheet", "chric"),
    ("informed_reconsent", "chric"),
    ("missing_data", "chrmiss"),
    ("recruitment_source", "chrrecruit"),
    ("coenrollment_form", "chrcoen"),
    ("lifetime_ap_exposure_screen", "chrap"),
    ("past_pharmaceutical_treatment", "chrpharm"),
    (
        "current_pharmaceutical_treatment_floating_med_125",
        "chrpharm",
    ),
    (
        "current_pharmaceutical_treatment_floating_med_2650",
        "chrpharm",
    ),
    ("resource_use_log", "chrrul"),
    ("sociodemographics", "chrdemo"),
    ("psychosocial_treatment_form", "chrpsychsoc"),
    ("health_conditions_medicalpsychiatric_history", "chrmed"),
    ("health_conditions_genetics_fluid_biomarkers", "chrhealth"),
    ("scid5_psychosis_mood_substance_abuse", "chrscid"),
    ("scid5_schizotypal_personality_sciddpq", "chrschizotypal"),
    ("traumatic_brain_injury_screen", "chrtbi"),
    ("family_interview_for_genetic_studies_figs", "chrfigs"),
    ("adverse_events", "chrae"),
    ("sofas_screening", "chrsofas"),
    ("sofas_followup", "chrsofas"),
    ("psychs_av_recording_run_sheet", "chrpsychs_av"),
    ("psychs_p1p8", "chrpsychs_scr"),
    ("psychs_p9ac32", "chrpsychs_scr"),
    ("psychs_p1p8_fu", "chrpsychs_fu"),
    ("psychs_p9ac32_fu", "chrpsychs_fu"),
    ("psychs_p1p8_fu_hc", "chrpsychs_fu"),
    ("psychs_p9ac32_fu_hc", "chrpsychs_fu"),
    ("inclusionexclusion_criteria_review", "chrcrit"),
    ("premorbid_adjustment_scale", "chrpas"),
    ("perceived_discrimination_scale", "chrdim"),
    ("pubertal_developmental_scale", "chrpds"),
    ("penncnb", "chrpenn"),
    ("iq_assessment_wasiii_wiscv_waisiv", "chriq"),
    ("premorbid_iq_reading_accuracy", "chrpreiq"),
    ("eeg_run_sheet", "chreeg"),
    ("mri_run_sheet", "chrmri"),
    ("mri_incidental_findings_run_sheet", "chrif"),
    ("digital_biomarkers_mindlamp_onboarding", "chrdbb"),
    ("digital_biomarkers_mindlamp_checkin", "chrdig"),
    (
        "digital_biomarkers_mindlamp_end_of_12month__study_p",
        "chrdbe",
    ),
    (
        "digital_biomarkers_mindlamp_end_of_12month_study_p",
        "chrdbe",
    ),
    ("digital_biomarkers_axivity_onboarding", "chrax"),
    ("digital_biomarkers_axivity_checkin", "chraxci"),
    (
        "digital_biomarkers_axivity_end_of_12month_study_pe",
        "chraxe",
    ),
    (
        "digital_biomarkers_axivity_end_of_12month__study_pe",
        "chraxe",
    ),
    ("current_health_status", "chrchs"),
    (
        "daily_activity_and_saliva_sample_collection",
        "chrsaliva",
    ),
    ("blood_sample_preanalytic_quality_assurance", "chrblood"),
    ("cbc_with_differential", "chrcbc"),
    ("gcp_cbc_with_differential", "chrgcp"),
    ("gcp_current_health_status", "chrgcp"),
    ("speech_sampling_run_sheet", "chrspeech"),
    ("nsipr", "chrnsipr"),
    ("cdss", "chrcdss"),
    ("oasis", "chroasis"),
    ("assist", "chrassist"),
    ("cssrs_baseline", "chrcssrsb"),
    ("cssrs_followup", "chrcssrsfu"),
    ("perceived_stress_scale", "chrpss"),
    ("global_functioning_social_scale", "chrgfss"),
    (
        "global_functioning_social_scale_followup",
        "chrgfssfu",
    ),
    ("global_functioning_role_scale", "chrgfrs"),
    ("global_functioning_role_scale_followup", "chrgfrsfu"),
    ("item_promis_for_sleep", "chrpromis"),
    ("bprs", "chrbprs"),
    ("pgis", "chrpgi"),
    ("psychosis_polyrisk_score", "chrpps"),
    ("ra_prediction", "chrpred"),
    ("guid_form", "chrguid"),
    ("conversion_form", "chrconv"),
];

// # Similar List at:
// # https://github.com/AMP-SCZ/utility/blob/main/rpms_form_labels.csv
pub const RPMS_TO_REDCAP_FORM_NAME: &[(&str, &str)] = &[
    ("Actigraphy", "digital_biomarkers_axivity_checkin"),
    ("AdverseEvents", "adverse_events"),
    ("ASSIST", "assist"),
    (
        "BloodSpecimenPQA",
        "blood_sample_preanalytic_quality_assurance",
    ),
    ("BPRS", "bprs"),
    ("CBC", "cbc_with_differential"),
    ("CBCwithDifferential", "cbc_with_differential"),
    ("CBC_GCP", "gcp_cbc_with_differential"),
    ("CDSS", "cdss"),
    ("Coenrollment", "coenrollment_form"),
    ("ConversionForm", "conversion_form"),
    ("CSSRS", "cssrs_followup"),
    ("CurrentHealthStatus", "current_health_status"),
    ("CurrentHealthStatus_GCP", "gcp_current_health_status"),
    ("Demographics", "sociodemographics"),
    ("EEG", "eeg_run_sheet"),
    ("EMA", "digital_biomarkers_mindlamp_checkin"),
    ("FIGS", "family_interview_for_genetic_studies_figs"),
    ("GlobalFuncRS", "global_functioning_role_scale"),
    ("GlobalFuncRSFUP", "global_functioning_role_scale_followup"),
    ("GlobalFuncSS", "global_functioning_social_scale"),
    (
        "GlobalFuncSSFUP",
        "global_functioning_social_scale_followup",
    ),
    ("GUID", "guid_form"),
    (
        "HealthConditions",
        "health_conditions_genetics_fluid_biomarkers",
    ),
    (
        "InclusionExclusionCriteriaReview",
        "inclusionexclusion_criteria_review",
    ),
    ("IQ Assessment", "iq_assessment_wasiii_wiscv_waisiv"),
    ("LifetimeAP", "lifetime_ap_exposure_screen"),
    ("MissingData", "missing_data"),
    ("MRI", "mri_run_sheet"),
    ("MRI-Incidental", "mri_incidental_findings_run_sheet"),
    ("NSI-PR", "nsipr"),
    ("OASIS", "oasis"),
    ("PAS", "premorbid_adjustment_scale"),
    ("PDQ", "perceived_discrimination_scale"),
    ("PDS", "pubertal_developmental_scale"),
    ("PubertalDevelopmentScale", "pubertal_developmental_scale"),
    ("PennCNB", "penncnb"),
    ("PGI-S", "pgis"),
    ("PharmaceuticalTreatment", "past_pharmaceutical_treatment"),
    ("PICF_HealthyControl_Self_V2", "informed_consent_run_sheet"),
    ("PICF_HealthyControl_Self_V3", "informed_consent_run_sheet"),
    ("PICF_HealthyControl_Self_V4", "informed_consent_run_sheet"),
    (
        "PICF_HealthyControl_ParentGuardian_V2",
        "informed_consent_run_sheet",
    ),
    (
        "PICF_HealthyControl_ParentGuardian_V3",
        "informed_consent_run_sheet",
    ),
    (
        "PICF_HealthyControl_ParentGuardian_V4",
        "informed_consent_run_sheet",
    ),
    ("PICF_UHR_Self_V2", "informed_consent_run_sheet"),
    ("PICF_UHR_Self_V3", "informed_consent_run_sheet"),
    ("PICF_UHR_Self_V4", "informed_consent_run_sheet"),
    ("PICF_UHR_ParentGuardian_V2", "informed_consent_run_sheet"),
    ("PICF_UHR_ParentGuardian_V3", "informed_consent_run_sheet"),
    ("PICF_UHR_ParentGuardian_V4", "informed_consent_run_sheet"),
    ("PPS", "psychosis_polyrisk_score"),
    ("PremorbidIQ", "premorbid_iq_reading_accuracy"),
    ("PROMIS-SD", "item_promis_for_sleep"),
    ("PSS", "perceived_stress_scale"),
    ("PsychosocialTreatment", "psychosocial_treatment_form"),
    ("PSYCHSP1P8", "psychs_p1p8_fu"),
    ("PSYCHSP9", "psychs_p9ac32_fu"),
    ("PSYCHSP1P8_v1", "psychs_p1p8_fu"),
    ("PSYCHSP1P8_v2", "psychs_p1p8_fu"),
    ("PSYCHSP9_v1", "psychs_p9ac32_fu"),
    ("PSYCHSP9_v2", "psychs_p9ac32_fu"),
    ("PsychsRunsheet", "psychs_av_recording_run_sheet"),
    ("RA Prediction", "ra_prediction"),
    ("RecruitmentSource", "recruitment_source"),
    ("RUL", "resource_use_log"),
    ("Saliva", "daily_activity_and_saliva_sample_collection"),
    ("SCID", "scid5_psychosis_mood_substance_abuse"),
    ("SCIDv1", "scid5_psychosis_mood_substance_abuse"),
    ("SCID5PD", "scid5_schizotypal_personality_sciddpq"),
    ("SOFAS", "sofas_followup"),
    ("SpeechSampling", "speech_sampling_run_sheet"),
    ("TBI", "traumatic_brain_injury_screen"),
];

pub fn form_name_to_rpms_suffix(key: &str) -> Option<&'static str> {
    hashify::tiny_map! {
        key.as_bytes(),
        "informed_consent_run_sheet" => "informed_consent_run_sheet.csv",
        "adverse_events" => "adverse_events.csv.flat",
        "assist" => "assist.csv",
        "blood_sample_preanalytic_quality_assurance" => "blood_sample_preanalytic_quality_assurance.csv.flat",
        "bprs" => "bprs.csv",
        "cbc_with_differential" => "cbc_with_differential.csv",
        "cdss" => "cdss.csv",
        "coenrollment_form" => "coenrollment_form.csv",
        "conversion_form" => "conversion_form.csv",
        "cssrs_baseline" => "cssrs_baseline.csv",
        "cssrs_followup" => "cssrs_followup.csv",
        "current_health_status" => "current_health_status.csv",
        "current_pharmaceutical_treatment_floating_med_125" => "current_pharmaceutical_treatment_floating_med_125.csv.flat",
        "current_pharmaceutical_treatment_floating_med_2650" => "current_pharmaceutical_treatment_floating_med_2650.csv.flat",
        "daily_activity_and_saliva_sample_collection" => "daily_activity_and_saliva_sample_collection.csv",
        "digital_biomarkers_axivity_checkin" => "digital_biomarkers_axivity_checkin.csv",
        "digital_biomarkers_axivity_end_of_12month__study_pe" => "digital_biomarkers_axivity_end_of_12month_study_pe.csv",
        "digital_biomarkers_axivity_onboarding" => "digital_biomarkers_axivity_onboarding.csv",
        "digital_biomarkers_mindlamp_checkin" => "digital_biomarkers_mindlamp_checkin.csv",
        "digital_biomarkers_mindlamp_end_of_12month__study_p" => "digital_biomarkers_mindlamp_end_of_12month_study_p.csv",
        "digital_biomarkers_mindlamp_onboarding" => "digital_biomarkers_mindlamp_onboarding.csv",
        "eeg_run_sheet" => "eeg_run_sheet.csv",
        "family_interview_for_genetic_studies_figs" => "family_interview_for_genetic_studies_figs.csv.flat",
        "gcp_cbc_with_differential" => "gcp_cbc_with_differential.csv",
        "gcp_current_health_status" => "gcp_current_health_status.csv",
        "global_functioning_role_scale" => "global_functioning_role_scale.csv",
        "global_functioning_role_scale_followup" => "global_functioning_role_scale_followup.csv",
        "global_functioning_social_scale" => "global_functioning_social_scale.csv",
        "global_functioning_social_scale_followup" => "global_functioning_social_scale_followup.csv",
        "guid_form" => "guid_form.csv",
        "health_conditions_genetics_fluid_biomarkers" => "health_conditions_genetics_fluid_biomarkers.csv",
        "health_conditions_medicalpsychiatric_history" => "health_conditions_medical_historypsychiatric_histo.csv.flat",
        "inclusionexclusion_criteria_review" => "inclusionexclusion_criteria_review.csv",
        // 'informed_reconsent',
        "iq_assessment_wasiii_wiscv_waisiv" => "iq_assessment_wasiii_wiscv_waisiv.csv",
        "item_promis_for_sleep" => "item_promis_for_sleep.csv",
        "lifetime_ap_exposure_screen" => "lifetime_ap_exposure_screen.csv",
        "missing_data" => "missing_data.csv",
        // 'mri_incidental_findings_run_sheet',
        "mri_run_sheet" => "mri_run_sheet.csv",
        "nsipr" => "nsipr.csv",
        "oasis" => "oasis.csv",
        "past_pharmaceutical_treatment" => "past_pharmaceutical_treatment.csv.flat",
        "penncnb" => "penncnb.csv",
        "perceived_discrimination_scale" => "perceived_discrimination_scale.csv",
        "perceived_stress_scale" => "perceived_stress_scale.csv",
        "pgis" => "pgis.csv",
        "premorbid_adjustment_scale" => "premorbid_adjustment_scale.csv",
        "premorbid_iq_reading_accuracy" => "premorbid_iq_reading_accuracy.csv",
        "psychosis_polyrisk_score" => "psychosis_polyrisk_score.csv",
        "psychosocial_treatment_form" => "psychosocial_treatment_form.csv.flat",
        "psychs_av_recording_run_sheet" => "psychs_av_recording_run_sheet.csv",
        "psychs_p1p8" => "psychs_p1p8.csv",
        "psychs_p1p8_fu" => "psychs_p1p8_fu.csv",
        "psychs_p1p8_fu_hc" => "psychs_p1p8_fu_hc.csv",
        "psychs_p9ac32" => "psychs_p9ac32.csv",
        "psychs_p9ac32_fu" => "psychs_p9ac32_fu.csv",
        "psychs_p9ac32_fu_hc" => "psychs_p9ac32_fu_hc.csv",
        "pubertal_developmental_scale" => "pubertal_developmental_scale.csv",
        "ra_prediction" => "ra_prediction.csv",
        "recruitment_source" => "recruitment_source.csv",
        "resource_use_log" => "resource_use_log.csv.flat",
        "scid5_psychosis_mood_substance_abuse" => "scid5_psychosis_mood_substance_abuse.csv",
        "scid5_schizotypal_personality_sciddpq" => "scid5_schizotypal_personality_sciddpq.csv",
        "sociodemographics" => "sociodemographics.csv",
        "sofas_followup" => "sofas_followup.csv",
        "sofas_screening" => "sofas_screening.csv",
        "speech_sampling_run_sheet" => "speech_sampling_run_sheet.csv",
        "traumatic_brain_injury_screen" => "traumatic_brain_injury_screen.csv.flat",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpms_to_redcap_event() {
        assert_eq!(rpms_to_redcap_event(0), Some("unknown"));
        assert_eq!(rpms_to_redcap_event(1), Some("screening"));
        assert_eq!(rpms_to_redcap_event(2), Some("baseline"));
        assert_eq!(rpms_to_redcap_event(17), Some("other_study"));
        assert_eq!(rpms_to_redcap_event(22), Some("other_study"));
        assert_eq!(rpms_to_redcap_event(98), Some("conversion"));
        assert_eq!(rpms_to_redcap_event(99), Some("floating_forms"));
        assert_eq!(rpms_to_redcap_event(100), Some("screening"));
        assert_eq!(rpms_to_redcap_event(101), Some("screening"));
        assert_eq!(rpms_to_redcap_event(999), None);
    }

    #[test]
    fn test_form_name_to_rpms_suffix() {
        assert_eq!(
            form_name_to_rpms_suffix("informed_consent_run_sheet"),
            Some("informed_consent_run_sheet.csv")
        );
        assert_eq!(
            form_name_to_rpms_suffix("adverse_events"),
            Some("adverse_events.csv.flat")
        );
        assert_eq!(form_name_to_rpms_suffix("assist"), Some("assist.csv"));
    }

    #[test]
    fn test_rpms_to_redcap_form_name() {
        assert!(
            RPMS_TO_REDCAP_FORM_NAME
                .contains(&("Actigraphy", "digital_biomarkers_axivity_checkin"))
        );
        assert!(RPMS_TO_REDCAP_FORM_NAME.contains(&("CBC_GCP", "gcp_cbc_with_differential")));
        assert!(
            RPMS_TO_REDCAP_FORM_NAME
                .contains(&("PICF_UHR_ParentGuardian_V4", "informed_consent_run_sheet"))
        );
        assert!(RPMS_TO_REDCAP_FORM_NAME.contains(&("PSYCHSP9_v2", "psychs_p9ac32_fu")));
        assert!(RPMS_TO_REDCAP_FORM_NAME.contains(&("TBI", "traumatic_brain_injury_screen")));
    }
}
