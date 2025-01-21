import numpy as np
import cvxpy as cp


if __name__ == "__main__":
    # Problem data.
    cost = [[0.14898845, 0.14898845, 0.14898845],
            [0.19368499, 0.19368499, 0.19368499],
            [0.1638873, 0.1638873, 0.1638873],
            [0.19368499, 0.19368499, 0.19368499],
            [0.22348268, 0.22348268, 0.22348268],
            [0.26817921, 0.26817921, 0.26817921],
            [0.25328037, 0.25328037, 0.25328037],
            [0.22348268, 0.22348268, 0.22348268],
            [0.28307806, 0.28307806, 0.28307806],
            [0.34267344, 0.34267344, 0.34267344],
            [0.1638873, 0.1638873, 0.1638873],
            [0.2979769, 0.2979769, 0.2979769],
            [0.28307806, 0.28307806, 0.28307806],
            [0.04469654, 0.04469654, 0.04469654],
            [0.34267344, 0.34267344, 0.34267344],
            [0.26817921, 0.26817921, 0.26817921],
            [0.11919076, 0.11919076, 0.11919076],
            [0.1638873, 0.1638873, 0.1638873],
            [0.11919076, 0.11919076, 0.11919076]]
    cost = np.array(cost)
    bounds = [[250000.,      0.,      0.],
              [0., 150000.,      0.],
              [250000.,      0.,      0.],
              [0.,      0., 300000.],
              [0., 260000.,      0.],
              [120000.,      0.,      0.],
              [350000.,      0.,      0.],
              [150000.,      0.,      0.],
              [0., 400000.,      0.],
              [0., 200000.,      0.],
              [250000.,      0.,      0.],
              [130000.,      0.,      0.],
              [0.,      0., 280000.],
              [0., 150000.,      0.],
              [0.,      0., 100000.],
              [0.,      0., 120000.],
              [0.,      0., 260000.],
              [125000.,      0.,      0.],
              [0., 300000.,      0.]]
    bounds = np.array(bounds)
A = np.clip(bounds, a_min=0, a_max=1)
b = [274999.99995318, 14999.99995318, 129999.99995318]
b = np.array(b)

# Construct the problem.
m, n = np.shape(A)
x = cp.Variable((m, n))

# *, +, -, / are overloaded to construct CVXPY objects.
objective = cp.Minimize(cp.sum(cost.T@cp.abs(x)))
# <=, >=, == are overloaded to construct CVXPY constraints.
constraints = [cp.sum(A.T@x, axis=0) == b, -bounds <= x, x <= bounds]
prob = cp.Problem(objective, constraints)

# The optimal objective is returned by prob.solve().
result = prob.solve(solver=cp.CLARABEL, verbose=True)
print(result)

# The optimal value for x is stored in x.value.
print(b)
print(cost)
print(np.round(x.value, 0))
print(np.sum(A.T@x.value, axis=0))

# The optimal Lagrange multiplier for a constraint
# is stored in constraint.dual_value.
# print(constraints[0].dual_value)
