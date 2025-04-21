from sklearn.base import BaseEstimator, TransformerMixin
import numpy as np

class FeatureCreator(BaseEstimator, TransformerMixin):
	# Create 2 new features from four existing ones

    def __init__(self):
        pass

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        
        necessary_features = ['px_width','px_height','sc_w', 'sc_h']

        if not all([feature in X.columns for feature in necessary_features]):
            raise ValueError('Features not present in the list')

        X = X.copy()

        num_pix = (X['px_width']*X['px_height'])
        aspect_ratio = (X['sc_w']/X['sc_h'])
        
        X['num_pix'] = num_pix
        X['aspect_ratio'] = aspect_ratio

        return X