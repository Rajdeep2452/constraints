from helper import Helper
helper = Helper()

class Validation:
    # Validate rule names
    valid_rules = set([
        "new_patients_expected_in_the_next_3_months",
        "new_patient_starts_in_a_particular_lot",
        "decline_in_rx_share_in_the_last_one_month",
        "switch_to_competitor_drug",
        "high_value_website_visits_in_the_last_15_days",
        "clicked_rep_triggered_email",
        "clicked_home_office_email",
        "clicked_3rd_party_email",
        "low_call_plan_attainment",
        "no_explicit_consent"
    ])

    rule_cols = {
        "new_patients_expected_in_the_next_3_months": "New_patients_in_next_quarter",
        "no_explicit_consent": "No_Consent",
        "clicked_3rd_party_email": "Clicked_3rd_Party_Email",
        "low_call_plan_attainment": "Best_time",
        "clicked_home_office_email": "Clicked_Home_Office_Email",
        "switch_to_competitor_drug": "Switch_to_Competitor",
        "new_patient_starts_in_a_particular_lot": "New_patients_in_particular_LOT",
        "high_value_website_visits_in_the_last_15_days": "High Value Website Visits",
        "decline_in_rx_share_in_the_last_one_month": "Decline_in_Rx_share_in_the_last_one_month",
        "clicked_rep_triggered_email": "Clicked_Rep_Email"
    }

    def _validate_conditions_hcp(data):
        if (data['Calls_Traditionalist'] == data['RTE_Traditionalist'] or
            data['Calls_Traditionalist'] == data['HOE_Traditionalist'] or
            data['Calls_Traditionalist'] == data['3P_Media_Traditionalist'] or
            data['RTE_Traditionalist'] == data['HOE_Traditionalist'] or
            data['RTE_Traditionalist'] == data['3P_Media_Traditionalist'] or
            data['HOE_Traditionalist'] == data['3P_Media_Traditionalist']):
            print("A")
            return False

        if (data['Calls_Digital_savvy'] == data['RTE_Digital_savvy'] or
            data['Calls_Digital_savvy'] == data['HOE_Digital_savvy'] or
            data['Calls_Digital_savvy'] == data['3P_Media_Digital_savvy'] or
            data['RTE_Digital_savvy'] == data['HOE_Digital_savvy'] or
            data['RTE_Digital_savvy'] == data['3P_Media_Digital_savvy'] or
            data['HOE_Digital_savvy'] == data['3P_Media_Digital_savvy']):
            print("B")
            return False

        if (data['Calls_Hybrid'] == data['RTE_Hybrid'] or
            data['Calls_Hybrid'] == data['HOE_Hybrid'] or
            data['Calls_Hybrid'] == data['3P_Media_Hybrid'] or
            data['RTE_Hybrid'] == data['HOE_Hybrid'] or
            data['RTE_Hybrid'] == data['3P_Media_Hybrid'] or
            data['HOE_Hybrid'] == data['3P_Media_Hybrid']):
            print("C")
            return False

        if (data['Calls_Status'] is None or
            data['HOE_Status'] is None or 
            data['RTE_Status'] is None or 
            data['3P_Status'] is None or 
            not isinstance(data['Calls_Status'], bool) or 
            not isinstance(data['RTE_Status'], bool) or 
            not isinstance(data['HOE_Status'], bool) or 
            not isinstance(data['3P_Status'], bool)):
            print("D")
            return False

        return True

    def validate_priority_order(status, priority_order, existing_priority_orders):
        if not isinstance(priority_order, int):
            return False, 'Priority_Order should be an integer.'

        if status and priority_order == 0:
            return False, f'Rules with status true cannot have priority order 0'

        if status and priority_order in existing_priority_orders:
            return False, f'Each rule should have a unique Priority_Order. Duplicate - {priority_order}'

        return True, None

    def validate_data_pt(self, rule, data):
        priority_order = data.get('Priority_Order', 1)  # Default value is 1 if not provided
        status = data.get('Status', False)  # Default value is False if not provided
        trigger_value = data.get('Trigger_Value', None)
        trigger_urgency = data.get('Trigger_Urgency', "Normal")  # Default value is "Normal" if not provided
        default_channel = data.get('Default_Channel', None)
        only_for_targets = data.get('Only_For_Targets', False)  # Default value is False if not provided

        # Validate priority order
        is_valid_priority_order, error_message = self.validate_priority_order(status, priority_order, Helper.existing_priority_orders)
        if not is_valid_priority_order:
            return False, error_message

        # Update the existing priority orders
        Helper.existing_priority_orders.add(priority_order)

        # Validate status
        if not isinstance(status, bool):
            return False, 'Status should be a boolean.'

        # Validate trigger value based on rules
        if rule in ["new_patients_expected_in_the_next_3_months",
                    "new_patient_starts_in_a_particular_lot",
                    "decline_in_rx_share_in_the_last_one_month",
                    "low_call_plan_attainment"]:
            if type(trigger_value) is not int:
                return False, f'Invalid Trigger_Value for rule {rule}: Should be an integer.'
        else:
            if type(trigger_value) is not bool:
                return False, f'Invalid Trigger_Value for rule {rule}: Should be a boolean.'

        # Validate trigger urgency
        if not isinstance(trigger_urgency, str):
            return False, 'Trigger_Urgency should be a string.'

        # Validate default channel
        if not isinstance(default_channel, str):
            return False, 'Default_Channel should be a string.'

        # Validate only for targets
        if not isinstance(only_for_targets, bool):
            return False, 'Only_For_Targets should be a boolean.'

        return True, None

    def _validate_post_data_suppression(post_data):
        # Validate common fields
        common_fields_valid = (
            1 <= post_data.get('vs_last_visit_completed', 0) <= 90 and
            1 <= post_data.get('vs_next_visit_planned', 0) <= 90 and
            1 <= post_data.get('rtes_last_rte_sent', 0) <= 90 and
            1 <= post_data.get('hoes_last_hoe_sent', 0) <= 90
        )

        # Validate 3pe_m* fields
        dynamic_fields_valid = all(
            isinstance(post_data.get(f'3pe_m{i}_value', 0), int) and
            1 <= post_data.get(f'3pe_m{i}_value', 0) <= 90 and
            isinstance(post_data.get(f'3pe_m{i}', ''), str)
            for i in range(1,  Helper._count_dynamic_fields(post_data) + 1)
        )

        return common_fields_valid and dynamic_fields_valid

