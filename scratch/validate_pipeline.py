import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from ml_engine.pipeline import train_all_models, predict_for_atm
import json

def test_pipeline():
    print("Testing train_all_models()...")
    results = train_all_models()
    print("Training Results:", json.dumps(results, indent=2))
    
    print("\nTesting predict_for_atm('NCB0001')...")
    pred = predict_for_atm("NCB0001")
    print("Prediction Result:", json.dumps(pred, indent=2))
    
if __name__ == "__main__":
    test_pipeline()
