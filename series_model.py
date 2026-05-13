# dota-poly-signal-pnl/series_model.py
def compute_bo3_match_p(
    p_current_map_yes: float,
    p_next_yes: float,
    series_score_yes: int,
    series_score_no: int,
    current_game_number: int,
    series_type: int = 1,
) -> float:
    """
    Computes the probability of the 'yes' team winning a Best-of-3 series.
    
    Args:
        p_current_map_yes: Probability of 'yes' team winning the current map.
        p_next_yes: Probability of 'yes' team winning a future map (assumed constant).
        series_score_yes: Current maps won by 'yes' team in the series.
        series_score_no: Current maps won by 'no' team in the series.
        current_game_number: The game number currently being played (1, 2, or 3).
        series_type: The series type code (1 for BO3 in this project).
        
    Returns:
        The total probability of the 'yes' team winning the series.
    """
    if series_type != 1:
        raise ValueError(f"Only series_type=1 (BO3) is supported, got {series_type}")
    
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
