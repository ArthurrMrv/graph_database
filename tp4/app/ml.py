from neo4j import GraphDatabase
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, f1_score
import matplotlib.pyplot as plt
import seaborn as sns
import os

class MLManager:
    def __init__(self, driver):
        self.driver = driver

    def train_and_evaluate(self):
        print("Fetching data for ML...")
        with self.driver.session() as session:
            # Fetch embeddings and languages
            result = session.run("""
                MATCH (s:Stream)
                WHERE s.embedding IS NOT NULL AND s.language IS NOT NULL
                RETURN s.id as id, s.language as language, s.embedding as embedding
            """)
            data = [record.data() for record in result]
        
        if not data:
            print("No data found for ML training. Check data loading and embedding generation.")
            return

        df = pd.DataFrame(data)
        
        # Prepare features (X) and target (y)
        X = np.array(df['embedding'].tolist())
        y = df['language']
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Train Classifier
        print("Training RandomForest Classifier...")
        clf = RandomForestClassifier(n_estimators=100, random_state=42)
        clf.fit(X_train, y_train)
        
        # Predict
        y_pred = clf.predict(X_test)
        
        # Evaluate
        print("\nClassification Report:")
        print(classification_report(y_test, y_pred))
        
        f1 = f1_score(y_test, y_pred, average='weighted')
        print(f"Weighted F1-Score: {f1}")
        
        # Generate Confusion Matrix
        self.plot_confusion_matrix(y_test, y_pred)
        
        return df

    def plot_confusion_matrix(self, y_test, y_pred):
        cm = confusion_matrix(y_test, y_pred)
        plt.figure(figsize=(10, 8))
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
        plt.title('Confusion Matrix')
        plt.ylabel('True Label')
        plt.xlabel('Predicted Label')
        
        output_dir = "/work/output"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        plt.savefig(f"{output_dir}/confusion_matrix.png")
        print(f"Confusion matrix saved to {output_dir}/confusion_matrix.png")
