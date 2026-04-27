"""
Визуализация результатов LOO-валидации.

Запуск:
    python -m scripts.validate_charts reports/validation_YYYYMMDD_HHMMSS.csv

Генерирует в той же папке:
    01_error_hist.png     — гистограмма распределения ошибок
    02_scatter.png        — факт vs прогноз
    03_mape_by_rooms.png  — MAPE по комнатности
    04_boxplot_rooms.png  — ящик с усами ошибок по комнатности
"""

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

plt.rcParams.update(
    {
        "font.family": "DejaVu Sans",
        "axes.spines.top": False,
        "axes.spines.right": False,
        "figure.dpi": 150,
    }
)

H3_COLOR = "#4C72B0"
KNN_COLOR = "#DD8452"
CLIP_PCT = 60  # обрезаем выбросы выше X% для гистограммы


def load(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df = df.dropna(subset=["h3_error_pct", "knn_error_pct"])
    df["rooms_label"] = df["rooms"].apply(
        lambda r: f"{r}-комн." if r < 5 else "5+ комн."
    )
    return df


# ── 1. Гистограмма ошибок ────────────────────────────────────────────────────


def plot_error_hist(df: pd.DataFrame, out: Path):
    h3 = df["h3_error_pct"].clip(upper=CLIP_PCT)
    knn = df["knn_error_pct"].clip(upper=CLIP_PCT)

    fig, ax = plt.subplots(figsize=(8, 4))
    bins = np.linspace(0, CLIP_PCT, 31)
    ax.hist(h3, bins=bins, alpha=0.6, color=H3_COLOR, label="H3-медиана")
    ax.hist(knn, bins=bins, alpha=0.6, color=KNN_COLOR, label="KNN (k=10)")

    for val, color, name in [
        (h3.median(), H3_COLOR, "медиана H3"),
        (knn.median(), KNN_COLOR, "медиана KNN"),
    ]:
        ax.axvline(
            val,
            color=color,
            linestyle="--",
            linewidth=1.5,
            label=f"{name} = {val:.1f}%",
        )

    ax.set_xlabel("Абсолютная ошибка, %")
    ax.set_ylabel("Число объявлений")
    ax.set_title("Распределение ошибок прогноза")
    ax.legend(fontsize=9)
    if h3.max() >= CLIP_PCT or knn.max() >= CLIP_PCT:
        ax.set_xlim(0, CLIP_PCT)
        ax.set_xlabel(f"Абсолютная ошибка, % (обрезано на {CLIP_PCT}%)")
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)
    print(f"  Сохранено: {out.name}")


# ── 2. Scatter: факт vs прогноз ──────────────────────────────────────────────


def plot_scatter(df: pd.DataFrame, out: Path):
    fig, axes = plt.subplots(1, 2, figsize=(11, 5))

    for ax, col, color, title in [
        (axes[0], "h3_predicted", H3_COLOR, "H3-медиана"),
        (axes[1], "knn_predicted", KNN_COLOR, "KNN (k=10)"),
    ]:
        x = df["actual_price"] / 1e6
        y = df[col] / 1e6
        ax.scatter(x, y, alpha=0.35, s=14, color=color)

        lim_max = max(x.max(), y.max()) * 1.05
        ax.plot([0, lim_max], [0, lim_max], "k--", linewidth=1, label="идеал")
        ax.set_xlim(0, lim_max)
        ax.set_ylim(0, lim_max)
        ax.set_xlabel("Фактическая цена, млн руб.")
        ax.set_ylabel("Прогноз, млн руб.")
        ax.set_title(title)
        ax.legend(fontsize=9)
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}"))
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}"))

    fig.suptitle("Факт vs Прогноз", fontsize=13)
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)
    print(f"  Сохранено: {out.name}")


# ── 3. MAPE по комнатности ───────────────────────────────────────────────────


def plot_mape_by_rooms(df: pd.DataFrame, out: Path):
    order = sorted(
        df["rooms_label"].unique(), key=lambda s: int(s[0]) if s[0].isdigit() else 99
    )
    h3_mape = df.groupby("rooms_label")["h3_error_pct"].mean().reindex(order)
    knn_mape = df.groupby("rooms_label")["knn_error_pct"].mean().reindex(order)
    counts = df.groupby("rooms_label")["listing_id"].count().reindex(order)

    x = np.arange(len(order))
    w = 0.35
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(x - w / 2, h3_mape, w, label="H3-медиана", color=H3_COLOR, alpha=0.85)
    ax.bar(x + w / 2, knn_mape, w, label="KNN (k=10)", color=KNN_COLOR, alpha=0.85)

    for i, (h, k, n) in enumerate(zip(h3_mape, knn_mape, counts)):
        ax.text(i - w / 2, h + 0.3, f"{h:.1f}%", ha="center", va="bottom", fontsize=8)
        ax.text(i + w / 2, k + 0.3, f"{k:.1f}%", ha="center", va="bottom", fontsize=8)
        ax.text(i, 0.5, f"n={n}", ha="center", va="bottom", fontsize=7, color="gray")

    ax.set_xticks(x)
    ax.set_xticklabels(order)
    ax.set_ylabel("MAPE, %")
    ax.set_title("Средняя ошибка (MAPE) по комнатности")
    ax.legend(fontsize=9)
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)
    print(f"  Сохранено: {out.name}")


# ── Итоговая таблица метрик ──────────────────────────────────────────────────


def print_metrics(df: pd.DataFrame):
    print("\n" + "=" * 58)
    print(f"  {'Метрика':<18} {'H3-медиана':>14} {'KNN (k=10)':>14}")
    print("=" * 58)

    h3e = df["h3_error_pct"]
    ke = df["knn_error_pct"]
    h3a = (df["h3_predicted"] - df["actual_price"]).abs()
    ka = (df["knn_predicted"] - df["actual_price"]).abs()

    rows = [
        ("MAPE", f"{h3e.mean():.2f}%", f"{ke.mean():.2f}%"),
        ("MedianAPE", f"{h3e.median():.2f}%", f"{ke.median():.2f}%"),
        ("MAE, млн руб.", f"{h3a.mean() / 1e6:.2f}", f"{ka.mean() / 1e6:.2f}"),
        (
            "RMSE, млн руб.",
            f"{(h3a**2).mean() ** 0.5 / 1e6:.2f}",
            f"{(ka**2).mean() ** 0.5 / 1e6:.2f}",
        ),
        ("n", str(len(df)), str(len(df))),
    ]
    for label, h3v, kv in rows:
        print(f"  {label:<18} {h3v:>14} {kv:>14}")
    print("=" * 58)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", help="Путь к CSV из scripts.validate")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"Файл не найден: {csv_path}")
        sys.exit(1)

    df = load(csv_path)
    out_dir = csv_path.parent
    stem = csv_path.stem

    print(f"Загружено {len(df)} объявлений из {csv_path.name}")
    print_metrics(df)

    print("\nГенерирую графики...")
    plot_error_hist(df, out_dir / f"{stem}_01_error_hist.png")
    plot_scatter(df, out_dir / f"{stem}_02_scatter.png")
    plot_mape_by_rooms(df, out_dir / f"{stem}_03_mape_by_rooms.png")


if __name__ == "__main__":
    main()
