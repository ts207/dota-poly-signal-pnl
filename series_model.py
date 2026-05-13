# dota-poly-signal-pnl/series_model.py
def compute_bo3_match_p(
    p_current_map_yes: float,
    p_next_yes: float,
    series_score_yes: int,
    series_score_no: int,
    current_game_number: int,
    series_type: int = 3,
) -> float:
    if series_type != 3:
        raise ValueError(f"Only series_type=3 (BO3) is supported, got {series_type}")
    
    if not (0.01 <= p_next_yes <= 0.99):
        raise ValueError(f"p_next_yes must be in [0.01, 0.99], got {p_next_yes}")

    # State validation
    valid = False
    if current_game_number == 1:
        valid = (series_score_yes == 0 and series_score_no == 0)
    elif current_game_number == 2:
        valid = (series_score_yes + series_score_no == 1)
    elif current_game_number == 3:
        valid = (series_score_yes == 1 and series_score_no == 1)
    
    if not valid:
        raise ValueError(f"Invalid BO3 state: Game {current_game_number} with score {series_score_yes}-{series_score_no}")

    p = p_current_map_yes
    q = p_next_yes
    
    if current_game_number == 1:
        return p * (q * (2 - q)) + (1 - p) * (q**2)
    elif current_game_number == 2:
        if series_score_yes == 1:
            return p + (1 - p) * q
        else:
            return p * q
    elif current_game_number == 3:
        return p
    
    raise ValueError("Unreachable state in compute_bo3_match_p")
