import pandas as pd
import numpy as np

import theano.tensor as tt 
from theano import shared

import pymc3 as pm 
import arviz as az 

from sklearn import datasets
import statsmodels.api as sm 

from matplotlib import pyplot as plt 

import time

features, target, coef = datasets.make_regression(n_samples=100, n_features=30, n_informative=30, n_targets=1, noise=0.0, coef=True)

coefs = pd.DataFrame(coef, columns=["true_coefs"])
features = pd.DataFrame(features, columns=["Feature_" + str(feat) for feat in range(features.shape[1])])
target = pd.DataFrame(target, columns=["target"])

with pm.Model() as model:
    # Priors
    intercept = pm.Normal("intercept", mu=0, sd=10)
    betas = pm.Normal("beta", mu=0, sd=10, shape=30)
    sigma = pm.HalfNormal("sigma", sd=10)

    # Deterministics
    mu = intercept + pm.math.dot(betas, features.T)

    # Likelihood
    Ylikelihood = pm.Normal("Ylikelihood", mu=mu, sd=sigma, observed=target)

# Draw posterior samples using NUTS Sampling
start_time = time.process_time()
trace = pm.sample(draws=7000, model=model, tune=3000, chains=4, cores=4, target_accept=0.9)
print(time.process_time() - start_time)

pm.plot_trace(trace)
plt.show()

