from mapping import load_valid_mappings

def debug_mappings():
    valid, errors = load_valid_mappings("dota-poly-signal-pnl/markets.yaml")
    print(f"Valid mappings: {len(valid)}")
    print(f"Errors: {len(errors)}")
    for err in errors:
        print(f"Error at index {err.index} ({err.name}): {err.reason}")

if __name__ == "__main__":
    debug_mappings()
