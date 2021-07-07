from ray import tune
import time

def objective(step, alpha, beta):
    # time.sleep(0.1)
    return (0.1 + alpha * step / 100)**(-1) + beta * 0.1


def training_function(config):
    # Hyperparameters
    alpha, beta = config["alpha"], config["beta"]
    for step in range(10):
        # Iterative training function - can be any arbitrary training procedure.
        intermediate_score = objective(step, alpha, beta)
        # Feed the score back back to Tune.
        tune.report(mean_loss=intermediate_score)

tstart = time.time()
analysis = tune.run(
    training_function,
    config={
        "alpha": tune.choice([-2000, 2000]),
        "beta": tune.choice([i for i in range(1, 1000)]),
        # "alpha": tune.uniform(0, 20),
        # "beta": tune.uniform(-100, 100),
        "activation": tune.grid_search(["relu", "tanh"]),
        "num_workers": 2,
        "num_cpus_per_worker": 1,
    },
    metric="mean_loss",
    mode="min",
    num_samples=50,
    checkpoint_freq=0,
    # checkpoint_at_end=True,
    stop={
        # "mean_loss": 1.0,
        "training_iteration": 100
    })
tend = time.time()
print("Best config: ", analysis.get_best_config(
    metric="mean_loss", mode="min"))
print("Total time: ", tend - tstart)

# Get a dataframe for analyzing trial results.
df = analysis.results_df