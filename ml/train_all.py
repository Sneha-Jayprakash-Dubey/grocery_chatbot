import subprocess
import sys
from pathlib import Path


def run(cmd):
    print(f"\n>>> Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def main():
    root = Path(__file__).resolve().parents[1]
    py = sys.executable
    run([py, str(root / "ml" / "generate_training_data.py")])
    run([py, str(root / "ml" / "train_intent.py")])
    run([py, str(root / "ml" / "train_recommender.py")])
    run([py, str(root / "ml" / "train_budget.py")])
    print("\nAll models trained and saved in ml/models.")


if __name__ == "__main__":
    main()
