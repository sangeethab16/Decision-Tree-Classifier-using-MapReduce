### Team Members

Sai Divya Sangeetha Bhagavatula

Maanasa Kaza

Prajakta Rodrigues

Anugraha Venugopal

Github Classroom Repo: https://github.com/2020-F-CS6240/project-group3

### Project Overview

High-energy particle colliders such as the Large Hadron Collider employed at CERN, the nuclear research centre, sometimes produce rare particles during collisions. These often lead to highly important scientific discoveries, making it essential to be able to identify these particles when they do form. However, most of the time, a lot of “background noise” is produced along with the “signal” - i.e., a lot of other particles, perhaps not of immediate interest in the ongoing research, are produced alongside the relevant ones. Consider the Higgs boson, a particle that gained widespread fame for explaining a major facet of why particles possess mass. When the Higgs boson is produced in a high-energy particle collider, it is often produced alongside a much larger proportion of particles not of interest. In our project, we look at this as a classification problem to differentiate between the process producing the Higgs bosons (signal process) and the process producing the noise (background process) We use a single decision tree trained on a very large data set in parallel to implement the binary classification.

### Input Data

The dataset used at this stage is a subset of the original dataset, consisting of 1091 instances (rows) and 28 attributes (columns). The first column is the class label (1 for signal, 0 for background). The next 21 columns are kinematic properties measured by the particle detectors in the accelerator. The last 6 features are functions of the first 21 features; these are high-level derived features used to help discriminate between the two classes. All the data values are real numbers.

### Task Name: Decision trees (MapReduce)

The goal of this task is to build a decision tree, train it on a very large data set in parallel, and classify outputs from particle collisions as signal or background noise. Existing libraries for trees will not be used.

### Overview


### Pseudo-Code


### Algorithm and Program Analysis


### Experiments


### Speedup


### Scalability


### Result Sample


### Conclusions

