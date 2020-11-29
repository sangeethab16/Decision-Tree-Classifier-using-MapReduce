### Team Members

Sai Divya Sangeetha Bhagavatula

Maanasa Kaza

Prajakta Rodrigues

Anugraha Venugopal

Github Classroom Repo: https://github.com/2020-F-CS6240/project-group3

### Project Overview

During the This is a classification problem to distinguish between a signal process which produces Higgs bosons and a background process which does not.

### Input Data

The data has been produced using Monte Carlo simulations. The first 21 features (columns 2-22) are kinematic properties measured by the particle detectors in the accelerator. The last seven features are functions of the first 21 features; these are high-level features derived by physicists to help discriminate between the two classes. There is an interest in using deep learning methods to obviate the need for physicists to manually develop such features. Benchmark results using Bayesian Decision Trees from a standard physics package and 5-layer neural networks are presented in the original paper. The last 500,000 examples are used as a test set.

The first column is the class label (1 for signal, 0 for background), followed by the 28 features (21 low-level features then 7 high-level features): lepton pT, lepton eta, lepton phi, missing energy magnitude, missing energy phi, jet 1 pt, jet 1 eta, jet 1 phi, jet 1 b-tag, jet 2 pt, jet 2 eta, jet 2 phi, jet 2 b-tag, jet 3 pt, jet 3 eta, jet 3 phi, jet 3 b-tag, jet 4 pt, jet 4 eta, jet 4 phi, jet 4 b-tag, m_jj, m_jjj, m_lv, m_jlv, m_bb, m_wbb, m_wwbb.

### Task Name: Decision trees (MapReduce)


### Overview


### Pseudo-Code


### Algorithm and Program Analysis


### Experiments


### Speedup


### Scalability


### Result Sample


### Conclusions

