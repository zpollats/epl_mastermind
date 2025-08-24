"""
Baseline ML Model for FPL Points Prediction (Fixed)
Time-series aware training with proper validation
"""

import duckdb
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Configuration
DB_PATH = "data/fpl_complete.db"
MODEL_DIR = Path("models")
MODEL_DIR.mkdir(exist_ok=True)

class FPLPredictor:
    """Fantasy Premier League points prediction model"""
    
    def __init__(self):
        self.model = None
        self.feature_columns = None
        self.feature_importance = None
        
    def load_data(self):
        """Load and prepare ML dataset with proper time ordering"""
        print("📊 Loading ML dataset...")
        
        conn = duckdb.connect(DB_PATH)
        
        # Load with proper time ordering
        query = """
        SELECT 
            player_name,
            season,
            gameweek,
            position_encoded,
            
            -- Target variable
            next_gw_points,
            
            -- Core features
            avg_points_5gw,
            avg_points_3gw,
            season_avg_points,
            consistency_score,
            blank_rate,
            big_haul_rate,
            points_per_90,
            games_played_to_date,
            
            -- Team features  
            team_form,
            team_attack,
            team_defense_weakness,
            
            -- Position features
            position_percentile,
            avg_position_points,
            
            -- Financial features
            player_value,
            ownership_pct,
            
            -- Binary features
            is_home,
            improving_form,
            consistent_performer,
            above_position_threshold,
            was_benched,
            played_full_game
            
        FROM mart_ml_features
        WHERE 
            next_gw_points IS NOT NULL
            AND avg_points_5gw IS NOT NULL
            AND games_played_to_date >= 5  -- Ensure good data quality
        ORDER BY season, gameweek, player_name
        """
        
        df = conn.execute(query).df()
        conn.close()
        
        print(f"✅ Loaded {len(df):,} records")
        print(f"   Players: {df['player_name'].nunique():,}")
        print(f"   Date range: {df['season'].min()} to {df['season'].max()}")
        print(f"   Gameweeks: {df['gameweek'].min()} to {df['gameweek'].max()}")
        
        return df
    
    def prepare_features(self, df):
        """Prepare features for model training"""
        print("🔧 Preparing features...")
        
        # Define feature columns (exclude identifiers and target)
        exclude_cols = ['player_name', 'season', 'gameweek', 'next_gw_points']
        self.feature_columns = [col for col in df.columns if col not in exclude_cols]
        
        # Handle any remaining missing values
        X = df[self.feature_columns].copy()
        y = df['next_gw_points'].copy()
        
        # Fill missing values with median (using pandas methods)
        for col in self.feature_columns:
            if X[col].dtype in ['float64', 'int64'] and X[col].isnull().any():
                median_val = X[col].median()
                X[col] = X[col].fillna(median_val)
                print(f"   Filled {X[col].isnull().sum()} missing values in {col}")
        
        print(f"✅ Features prepared: {len(self.feature_columns)} features")
        print(f"   Feature list: {self.feature_columns[:5]}..." + 
              (f" and {len(self.feature_columns)-5} more" if len(self.feature_columns) > 5 else ""))
        
        return X, y, df[['player_name', 'season', 'gameweek']].copy()
    
    def create_time_based_splits(self, df):
        """Create time-based train/validation/test splits"""
        print("📅 Creating time-based splits...")
        
        # Use seasons for splits (proper time series approach)
        train_seasons = ['2020-21', '2021-22', '2022-23']  # 3 seasons for training
        val_season = ['2023-24']                           # 1 season for validation  
        test_season = ['2024-25']                          # 1 season for testing
        
        train_mask = df['season'].isin(train_seasons)
        val_mask = df['season'].isin(val_season)  
        test_mask = df['season'].isin(test_season)
        
        print(f"   Train: {train_mask.sum():,} records ({train_seasons})")
        print(f"   Validation: {val_mask.sum():,} records ({val_season})")
        print(f"   Test: {test_mask.sum():,} records ({test_season})")
        
        return train_mask, val_mask, test_mask
    
    def train_baseline_model(self, X_train, y_train, X_val, y_val):
        """Train RandomForest baseline model"""
        print("🌳 Training RandomForest model...")
        
        # RandomForest with reasonable hyperparameters
        self.model = RandomForestRegressor(
            n_estimators=200,
            max_depth=10,
            min_samples_split=20,
            min_samples_leaf=10,
            random_state=42,
            n_jobs=-1
        )
        
        # Train the model
        self.model.fit(X_train, y_train)
        
        # Get feature importance
        self.feature_importance = pd.DataFrame({
            'feature': self.feature_columns,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        # Validation predictions
        y_val_pred = self.model.predict(X_val)
        
        # Validation metrics
        val_mae = mean_absolute_error(y_val, y_val_pred)
        val_rmse = np.sqrt(mean_squared_error(y_val, y_val_pred))
        val_r2 = r2_score(y_val, y_val_pred)
        
        print(f"✅ Model trained!")
        print(f"   Validation MAE: {val_mae:.3f}")
        print(f"   Validation RMSE: {val_rmse:.3f}")
        print(f"   Validation R²: {val_r2:.3f}")
        
        return val_mae, val_rmse, val_r2
    
    def evaluate_model(self, X_test, y_test, meta_test):
        """Comprehensive model evaluation"""
        print("📊 Evaluating model performance...")
        
        # Test predictions
        y_test_pred = self.model.predict(X_test)
        
        # Convert to numpy arrays to avoid pandas issues
        y_test_np = np.array(y_test)
        y_pred_np = np.array(y_test_pred)
        
        # Basic metrics
        test_mae = mean_absolute_error(y_test_np, y_pred_np)
        test_rmse = np.sqrt(mean_squared_error(y_test_np, y_pred_np))
        test_r2 = r2_score(y_test_np, y_pred_np)
        
        print(f"\n📈 Test Set Performance:")
        print(f"   MAE: {test_mae:.3f} points")
        print(f"   RMSE: {test_rmse:.3f} points")
        print(f"   R²: {test_r2:.3f}")
        
        # FPL-specific metrics
        print(f"\n⚽ FPL-Specific Analysis:")
        
        # Prediction vs actual distribution
        print(f"   Actual avg: {y_test_np.mean():.2f} ± {y_test_np.std():.2f}")
        print(f"   Predicted avg: {y_pred_np.mean():.2f} ± {y_pred_np.std():.2f}")
        
        # Directional accuracy (predicting above/below median performance)
        actual_median = np.median(y_test_np)
        pred_median = np.median(y_pred_np)
        actual_positive = (y_test_np > actual_median).astype(int)
        pred_positive = (y_pred_np > pred_median).astype(int)
        directional_acc = np.mean(actual_positive == pred_positive)
        print(f"   Directional accuracy: {directional_acc:.1%}")
        
        # Big haul prediction (10+ points)
        big_hauls_actual = np.sum(y_test_np >= 10)
        big_haul_threshold = np.percentile(y_pred_np, 95)
        big_hauls_pred_top = np.sum(y_pred_np >= big_haul_threshold)
        big_haul_overlap = np.sum((y_test_np >= 10) & (y_pred_np >= big_haul_threshold))
        
        print(f"   Big hauls (10+ pts): {big_hauls_actual} actual")
        print(f"   Top 5% predictions captured: {big_haul_overlap}/{big_hauls_actual} " + 
              f"({big_haul_overlap/max(big_hauls_actual,1):.1%})")
        
        # Feature importance
        print(f"\n🎯 Top 10 Most Important Features:")
        for _, row in self.feature_importance.head(10).iterrows():
            print(f"   {row['feature']}: {row['importance']:.3f}")
        
        return {
            'test_mae': test_mae,
            'test_rmse': test_rmse, 
            'test_r2': test_r2,
            'directional_accuracy': directional_acc,
            'big_haul_recall': big_haul_overlap/max(big_hauls_actual,1)
        }
    
    def save_model(self):
        """Save trained model and metadata"""
        model_path = MODEL_DIR / "baseline_rf_model.pkl"
        features_path = MODEL_DIR / "feature_columns.pkl"
        importance_path = MODEL_DIR / "feature_importance.csv"
        
        joblib.dump(self.model, model_path)
        joblib.dump(self.feature_columns, features_path) 
        self.feature_importance.to_csv(importance_path, index=False)
        
        print(f"💾 Model saved to {model_path}")

def main():
    """Main training pipeline"""
    print("🚀 FPL Baseline Model Training")
    print("=" * 50)
    
    # Initialize predictor
    predictor = FPLPredictor()
    
    # Load and prepare data
    df = predictor.load_data()
    X, y, meta = predictor.prepare_features(df)
    
    # Create time-based splits
    train_mask, val_mask, test_mask = predictor.create_time_based_splits(df)
    
    X_train, y_train = X[train_mask], y[train_mask]
    X_val, y_val = X[val_mask], y[val_mask]
    X_test, y_test = X[test_mask], y[test_mask]
    meta_test = meta[test_mask]
    
    # Train model
    val_metrics = predictor.train_baseline_model(X_train, y_train, X_val, y_val)
    
    # Evaluate model
    test_metrics = predictor.evaluate_model(X_test, y_test, meta_test)
    
    # Save model
    predictor.save_model()
    
    print("\n" + "=" * 50)
    print("✅ Baseline Model Training Complete!")
    print("=" * 50)
    print("\n🎯 Key Results:")
    print(f"   Test MAE: {test_metrics['test_mae']:.3f} points")
    print(f"   Directional accuracy: {test_metrics['directional_accuracy']:.1%}")
    print(f"   Big haul recall: {test_metrics['big_haul_recall']:.1%}")
    print("\n🚀 Ready for prediction pipeline development!")

if __name__ == "__main__":
    main()