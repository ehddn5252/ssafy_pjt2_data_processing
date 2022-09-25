d = {'balk': 4, # 보크
 'catcher_interf': 166, # 포수의 간섭으로 인한 1base 전진 (x)
 'caught_stealing_2b': 2731, # 2루로 가는 도루 저지 (pitcher) pitchers.CT_num
 'caught_stealing_3b': 134, # 3루로 가는 도루 저지 (pitcher) pitchers.CT_num
 'caught_stealing_home': 112, # 홈으로 가는 도루 저지 (pitchers) pitchers.CT_num
 'defensive_indiff': 1, # 수비적 무관심(도루를 시도해도 도루를 막지 않으려 함) (x)
 'defensive_substitution': 1, # 수비 교체 (x)
 'defensive_switch': 2, # 수비 쉬프트 (x)
 'double': 85658, # 2루타 (batters, pitchers) left_pa_num, batters.left_hit_num, batters.left_twob_hit_num, batters.left_ab_num
 'double_play': 5375, # 더블플레이 (batters, pitchers) left_pa_num, batters.left_dp_num, batters.left_ab_num
 'etc': 16, # 기타
 'field_error': 17463, # 수비 에러 (x)
 'field_out': 776381, # 아웃 (batters, pitchers) left_pa_num, batters.left_out_num, batters.left_ab_num
 'fielders_choice': 1860, # 야수 선택 (pitchers) left_pa_num, batters.left_out_num, batters.left_ab_num
 'fielders_choice_out': 3222, # 야수 선택 아웃 (batters, pitchers) left_pa_num, batters.left_out_num, batters.left_ab_num
 'force_out': 42325, # 포스플레이 아웃(루상에 발을 밞아서 아웃시킴), left_pa_num, batters.left_ab_num
 'grounded_into_double_play': 37073, # 땅볼로 인한 병살 (batters, pitchers) left_pa_num, batters.left_dp_num, batters.left_ab_num
 'hit_by_pitch': 16907, # 공에 맞음 ( pitchers, batters) left_pa_num, batters.left_bb_num pitchers 도 동일
 'home_run': 50814, # 홈런 (batters, pitchers) left_pa_num, batters.left_hr_num, batters.left_ab_num
 'injury': 1, # 부상 (x)
 'intent_walk': 12712 , # 고의 사구 (batters, pitchers) left_pa_num, batters.left_bb_num , batters.left_ibb_num
 'other_advance': 1, #(x)
 'other_out': 227, #(x)
 'passed_ball': 5, # 포수 책임으로 투수가 던진 볼을 못잡아 타자가 진루하는 경우 (x)
 'pickoff_1b': 459, # 1루 견제 (pitchers) (x) pitchers.pickoff_num
 'pickoff_2b': 140, # 2루 견제 (pitchers) (x) pitchers.pickoff_num
 'pickoff_3b': 18, # 3루 견제 (pitchers) (x) pitchers.pickoff_num
 'pickoff_caught_stealing_2b': 728, # 2루 견제 아웃 (pitchers) (x) pitchers.pickoff_catch_num
 'pickoff_caught_stealing_3b': 30, # 3루 견제 아웃 (pitchers) (x) pitchers.pickoff_catch_num
 'pickoff_caught_stealing_home': 23, # 홈 견제 아웃 (pitchers) (x) pitchers.pickoff_catch_num
 'pickoff_error_1b': 1, # 1루 견제 에러 (x)
 'pickoff_error_2b': 1, # 2루 견제 에러 (x)
 'pickoff_error_3b': 1, # 3루 견제 에러 (x)
 'pitching_substitution': 9, # (x)
 'runner_double_play': 8, # 주자로 인한 더블플레이
 'sac_bunt': 16062, # 희생번트 (batters)  left_pa_num,
 'sac_bunt_double_play': 14, # 희생번트로 인한 더블플레이 (batters, pitchers) left_pa_num, batters.left_dp_num
 'sac_fly': 13340, # 희생 플라이 (batters, pitchers)  left_pa_num,
 'sac_fly_double_play': 155, #희생 플레이로 인한 더블 플레이 (batters) left_pa_num, batters.left_dp_num
 'single': 284502, # 안타 (batters, pitchers) left_pa_num, batters.left_hit_num, batters.left_ab_num
 'stolen_base_2b': 20, # 2루 도루 (pitchers)
 'stolen_base_3b': 1, # 3루 도루 (batters, pitchers)
 'stolen_base_home': 2, # 홈 도루 (batters, pitchers)
 'strikeout': 302712, # 스트라이크 아웃 (batters, pitchers)
 'strikeout_double_play': 1804, # 스트라이크 아웃 + 도루 아웃 (batters, pitchers) left_pa_num, batters.left_ab_num
 'strikeout_triple_play': 2, # 스트라이크 아웃 + 도루 2번 다 아웃 (batters, pitchers) left_pa_num, batters.left_ab_num
 'triple': 9100, # 3루타 (batters, pitchers) left_pa_num, batters.left_ab_num batters.left_3base_hit_num, pitchers.left_3base_hit_num
 'triple_play': 31, # 트리플 아웃 (batters, pitchers)
 'walk': 145666, # 볼넷 (batters, pitchers) left_pa_num, batters.left_bb_num, pitchers.left_bb_num
 'wild_pitch': 38 # 와일드 피치 (pitchers)
 }

print(d.keys())