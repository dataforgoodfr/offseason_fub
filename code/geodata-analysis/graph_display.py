from textwrap import fill
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

# Graphique custom pour afficher les données
def plot_top_10(df, title, col_disp):
    tp = df.copy()

    tp["communes"] = (tp["communes"]
                      .str.replace("Arrondissement", "", regex=False)
                      .str.replace("  ,", "|", regex=False)
                      .str.replace(" , ", "|", regex=False))

    def wrap_label(s, width=36):
        return fill(s.replace("|", " | "), width=width)

    tp["communes_wrapped"] = tp["communes"].apply(lambda s: wrap_label(s, width=38))

    is_float = tp[col_disp].dtype.kind in {"f"}
    if is_float:
        vals = tp[col_disp].round(2)
        fmt_val = lambda v: f"{v:.2f}"
        xfmt = FuncFormatter(lambda x, pos: f"{x:.1f}")
    else:
        vals = tp[col_disp]
        fmt_val = lambda v: f"{int(v):,}".replace(",", " ")
        xfmt = FuncFormatter(lambda x, pos: f"{int(x):,}".replace(",", " "))

    fig, ax = plt.subplots(figsize=(11.5, 6.5), layout="constrained")
    bars = ax.barh(tp["communes_wrapped"], vals)

    ax.invert_yaxis()
    ax.set_title(title)
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    ax.xaxis.set_major_formatter(xfmt)

    xmin = 0 if vals.min() >= 0 else float(vals.min())
    xmax = float(vals.max())
    dx = (xmax - xmin) if (xmax - xmin) > 0 else 1.0
    offset = 0.02 * dx

    for bar, value in zip(bars, vals):
        ax.text(
            value + offset,
            bar.get_y() + bar.get_height()/2,
            fmt_val(value),
            va="center",
            ha="left",
            fontsize=10,
        )

    ax.set_xlim(left=min(0, xmin), right=xmax + 3*offset)

    plt.subplots_adjust(left=0.35)

    plt.show()