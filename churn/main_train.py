from src import train_model
import warnings
warnings.filterwarnings("ignore")

if __name__ == '__main__':
    for sem in range(6, 13):
        print(f'sem number {sem}')
        train_model(n_semester=sem)
