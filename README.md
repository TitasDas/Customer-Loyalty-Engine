# Loyalty-measures
Different approaches to measuring an ecommerce customer's loyalty. An initial report based on loyalty insights done through analyzing customer's transaction data can be found [here](https://drive.google.com/file/d/15xsNaw4A8VkDBwuShBj_V84xMuNZ37ty/view?usp=sharing).

## Algorithms
1. RFM - The different variations can be in the way recency, frequency and monetary value are calculated. Also can take lifetime value into consideration.  
2. RFM measure with returns taken into account
3. Causal directed graphs


### Causal Directed Graphs
1. [DoWhy](https://github.com/microsoft/dowhy) : A python library for causal inference , supports explicit modeling and causal assumptions can be tested as well. [official doc](https://microsoft.github.io/dowhy/) 

	- [Estimating the effect of a member rewards program](https://github.com/microsoft/dowhy/blob/master/docs/source/example_notebooks/dowhy_example_effect_of_memberrewards_program.ipynb) - can be done after we take advantage of loyalty or before? 
	
	- How long should an articles' headline be? [length of wordcount on news articles headline](https://medium.com/@akelleh/introducing-the-do-sampler-for-causal-inference-a3296ea9e78d) 
	
	- Hotel cancellations - can this be used for order cancellations or returns? - [Beyond Predictive Models: The Causal Story Behind Hotel Booking Cancellations](https://towardsdatascience.com/beyond-predictive-models-the-causal-story-behind-hotel-booking-cancellations-d29e8558cbaf) 

2. [Causalnex](https://github.com/quantumblacklabs/causalnex) : Python library that helps infer causation instead of observing correlation. 

3. Causual discovery [toolbox](https://github.com/FenTechSolutions/CausalDiscoveryToolbox) : Causal inference in graphs and in the pairwise settings. Also can do graph structure recovery and dependencies. 

4. [Causal graphical models ](https://github.com/ijmbarr/causalgraphicalmodels) : Python module built on top of networkx for causal graphs and structural graphs

### RFM
*1. Link to the lifetimes library - [todo]*



# Extensions

Within the domain of ecommerce, what else can be done after loyalty has been measured?

1. Introduction of loyalty rewards programs like amazon prime
2. Victory laps of customers
