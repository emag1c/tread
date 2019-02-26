import pandas as pd
import numpy as np

from . import DsyncServer


if __name__ == "__main__":
    df = pd.DataFrame(np.random.randn(50, 4), columns=list('ABCD'))

    ds = DsyncServer()