# Mod�le LSTM
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout, BatchNormalization
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint, ReduceLROnPlateau
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import pickle
import json
from datetime import datetime, timedelta
import config

class LSTMModel:
    def __init__(self, lookback=60):
        self.config = config.Config
        self.lookback = lookback
        self.model = None
        self.history = None
    
    def build_model(self, input_shape):
        """Construit le modèle LSTM"""
        model = Sequential([
            LSTM(128, return_sequences=True, input_shape=input_shape,
                 kernel_regularizer=tf.keras.regularizers.l2(0.001)),
            BatchNormalization(),
            Dropout(0.3),
            
            LSTM(64, return_sequences=True,
                 kernel_regularizer=tf.keras.regularizers.l2(0.001)),
            BatchNormalization(),
            Dropout(0.3),
            
            LSTM(32, return_sequences=False,
                 kernel_regularizer=tf.keras.regularizers.l2(0.001)),
            BatchNormalization(),
            Dropout(0.2),
            
            Dense(16, activation='relu'),
            Dropout(0.1),
            Dense(1)
        ])
        
        model.compile(
            optimizer=tf.keras.optimizers.Adam(learning_rate=0.001),
            loss='mse',
            metrics=['mae', 'mape']
        )
        
        return model
    
    def train(self, X_train, y_train, X_test, y_test):
        """Entraîne le modèle"""
        print("Entraînement du modèle LSTM...")
        
        input_shape = (X_train.shape[1], X_train.shape[2])
        self.model = self.build_model(input_shape)
        
        # Callbacks
        callbacks = [
            EarlyStopping(
                monitor='val_loss',
                patience=15,
                restore_best_weights=True,
                verbose=1
            ),
            ModelCheckpoint(
                str(self.config.GOLD_DIR / self.config.MODEL_FILE),
                monitor='val_loss',
                save_best_only=True,
                verbose=1
            ),
            ReduceLROnPlateau(
                monitor='val_loss',
                factor=0.5,
                patience=5,
                min_lr=0.00001,
                verbose=1
            )
        ]
        
        # Entraînement
        self.history = self.model.fit(
            X_train, y_train,
            validation_data=(X_test, y_test),
            epochs=self.config.EPOCHS,
            batch_size=self.config.BATCH_SIZE,
            callbacks=callbacks,
            verbose=1
        )
        
        print("Modèle entraîné avec succès!")
        return self.history
    
    def evaluate(self, X_test, y_test, scaler_y):
        """Évalue le modèle"""
        predictions_scaled = self.model.predict(X_test)
        predictions = scaler_y.inverse_transform(predictions_scaled)
        actual = scaler_y.inverse_transform(y_test.reshape(-1, 1))
        
        # Métriques
        mae = mean_absolute_error(actual, predictions)
        mse = mean_squared_error(actual, predictions)
        rmse = np.sqrt(mse)
        mape = np.mean(np.abs((actual - predictions) / actual)) * 100
        r2 = r2_score(actual, predictions)
        
        metrics = {
            'MAE': float(mae),
            'MSE': float(mse),
            'RMSE': float(rmse),
            'MAPE': float(mape),
            'R2': float(r2)
        }
        
        print(f"\nPerformance du modèle:")
        for metric, value in metrics.items():
            print(f"  {metric}: {value:.4f}")
        
        # Sauvegarde des métriques
        metrics_path = self.config.GOLD_DIR / "model_metrics.json"
        with open(metrics_path, 'w') as f:
            json.dump(metrics, f, indent=2)
        
        return metrics, predictions, actual
    
    def predict_future(self, last_sequence, scaler_X, scaler_y, days=3):
        """Prédit les prix futurs"""
        predictions = []
        current_sequence = last_sequence.copy()
        
        for day in range(days):
            # Prédiction - reshape en 3D pour le modèle LSTM
            input_sequence = current_sequence.reshape(1, current_sequence.shape[0], current_sequence.shape[1])
            pred_scaled = self.model.predict(input_sequence, verbose=0)
            pred = scaler_y.inverse_transform(pred_scaled)[0][0]
            
            # Date de prédiction
            pred_date = (datetime.now() + timedelta(days=day+1)).strftime('%Y-%m-%d')
            
            predictions.append({
                'day': day + 1,
                'date': pred_date,
                'predicted_price': float(pred),
                'confidence': 0.95  # Niveau de confiance fictif
            })
            
            # Mise à jour de la séquence pour la prédiction suivante
            # Décalage des lignes (supprimer la première, ajouter une nouvelle)
            new_row = current_sequence[-1, :].copy()
            
            # Décalage des valeurs de lag
            for i in range(len(new_row) - 1, 0, -1):
                new_row[i] = new_row[i-1]
            
            # Mise à jour avec la prédiction
            new_row[0] = scaler_y.transform([[pred]])[0][0]
            
            # Ajout à la séquence (roll et ajout de la nouvelle ligne)
            current_sequence = np.vstack([current_sequence[1:, :], new_row.reshape(1, -1)])
        
        
        return predictions

    @staticmethod
    def load_model(path):
        """Charge un modèle Keras sauvegardé et renvoie l'objet Keras."""
        from tensorflow.keras.models import load_model as _load_model
        try:
            # Charger sans compiler pour éviter les problèmes de métriques custom
            keras_model = _load_model(path, compile=False)
            return keras_model
        except Exception as e:
            print(f"✗ Impossible de charger le modèle Keras: {e}")
            return None