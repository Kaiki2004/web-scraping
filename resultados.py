#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gráficos e rankings a partir do MySQL mostrando APENAS Magalu e KaBuM!

Mudanças-chave:
- Após normalizar 'fornecedor', filtra o DataFrame para manter só ['Magalu', 'KaBuM!']
- Paleta fixa (Magalu azul, KaBuM! laranja)
- Restante idêntico ao resultados_full.py

Uso:
  pip install pandas numpy sqlalchemy pymysql matplotlib seaborn
  python resultados_mag_kabum_only.py --like "Galaxy S24"
"""

import os
import argparse
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sqlalchemy import create_engine

# ============== CONFIG (env com fallback) ==============
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASS = os.environ.get("MYSQL_PASS", "admin")
MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = os.environ.get("MYSQL_PORT", "3306")
MYSQL_DB   = os.environ.get("MYSQL_DB", "ecommerce_scraping")

OUTPUT_DIR = os.environ.get("SCRAPE_OUTPUT_DIR", "./outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Paleta fixa apenas para as duas lojas
PALETTE = {"Magalu": "#1f77b4", "KaBuM!": "#ff7f0e"}

sns.set_theme(context="notebook", style="whitegrid")

# ============== Helpers ==============
def _norm(s: str) -> str:
    return (s or "").strip().lower()

def normalize_fornecedor(name: str) -> str:
    """Agrupa rótulos em 'Magalu', 'KaBuM!' ou mantém original (antes do filtro)."""
    n = _norm(name)
    if not n:
        return "Desconhecido"
    if "magalu" in n or "magazineluiza" in n:
        return "Magalu"
    if "kabum" in n:
        return "KaBuM!"
    return (name or "").strip() or "Desconhecido"

# ============== Conexão e carga ==============
def get_engine():
    uri = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
    return create_engine(uri, pool_pre_ping=True)

def load_denormalized_only_main():
    """Lê e normaliza, depois mantém apenas Magalu/KaBuM!"""
    eng = get_engine()
    sql = """
    SELECT
        L.id_list, L.url, L.price, L.avaliacao, L.frete_price, L.prazo_entrega, L.created_at,
        F.id_fornecedor, F.name AS fornecedor,
        S.id_seller, S.name AS seller,
        P.id_product, P.brand, P.code AS product_code, P.model, P.variante
    FROM List L
    JOIN Fornecedores F ON F.id_fornecedor = L.fk_fornecedor
    JOIN Seller S ON S.id_seller = L.fk_seller
    JOIN Products P ON P.id_product = L.fk_product
    """
    df = pd.read_sql(sql, eng)

    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=False)
    df["date"] = df["created_at"].dt.date
    df["frete_price"] = pd.to_numeric(df["frete_price"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["avaliacao"] = pd.to_numeric(df["avaliacao"], errors="coerce")

    # normaliza fornecedor e filtra
    df["fornecedor"] = df["fornecedor"].astype(str).map(normalize_fornecedor)
    df = df[df["fornecedor"].isin(["Magalu", "KaBuM!"])].copy()

    # avaliações válidas
    df.loc[~df["avaliacao"].between(0, 5, inclusive="both"), "avaliacao"] = np.nan

    df["total_price"] = df["price"].fillna(0) + df["frete_price"].fillna(0)
    return df

# ============== Filtros e utilidades ==============
def filter_product(df, product_code=None, product_like=None):
    d = df.copy()
    if product_code:
        d = d[d["product_code"] == product_code]
    if product_like:
        like = str(product_like).strip()
        mask = (
            d["brand"].astype(str).str.contains(like, case=False, na=False) |
            d["model"].astype(str).str.contains(like, case=False, na=False)
        )
        d = d[mask]
    return d

def savefig(path):
    plt.tight_layout()
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()

# ============== Gráficos (somente Magalu/KaBuM!) ==============
def price_trend(df, by="fornecedor"):
    d = (
        df.groupby([by, "date"], as_index=False)["price"]
          .mean()
          .sort_values("date")
    )
    if d.empty:
        return None
    plt.figure(figsize=(9, 6))
    ax = sns.lineplot(data=d, x="date", y="price", hue=by, palette=PALETTE, marker="o")
    ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=4, maxticks=8))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
    plt.xticks(rotation=45, ha="right")
    plt.xlabel("Data")
    plt.ylabel("Preço médio (R$)")
    plt.title("Evolução de preço por fornecedor (Magalu x KaBuM!)")
    plt.legend(title="")
    out = os.path.join(OUTPUT_DIR, "01_price_trend.png")
    savefig(out)
    return out

def store_competition(df):
    d = (
        df.groupby("fornecedor", as_index=False)["price"]
          .mean()
          .loc[lambda x: x["fornecedor"].isin(["Magalu", "KaBuM!"])]
          .sort_values("price")
    )
    if d.empty:
        return None
    plt.figure(figsize=(9, 6))
    sns.barplot(
        data=d,
        x="fornecedor",
        y="price",
        order=["Magalu", "KaBuM!"] if set(d["fornecedor"])=={"Magalu","KaBuM!"} else d["fornecedor"],
        palette=[PALETTE.get(f, "#999999") for f in d["fornecedor"]],
    )
    plt.xlabel("Fornecedor")
    plt.ylabel("Preço médio (R$)")
    plt.title("Diferença de preço entre fornecedores (Magalu x KaBuM!)")
    plt.xticks(rotation=0)
    out = os.path.join(OUTPUT_DIR, "02_store_competition.png")
    savefig(out)
    return out

def price_vs_rating(df):
    d = df.dropna(subset=["price", "avaliacao"])
    if d.empty:
        return None
    plt.figure(figsize=(9, 6))
    ax = sns.scatterplot(
        data=d, x="avaliacao", y="price",
        hue="fornecedor", palette=PALETTE, alpha=0.85, s=70
    )
    try:
        sns.regplot(data=d, x="avaliacao", y="price", scatter=False, ax=ax, ci=None, color="#333333")
    except Exception:
        pass
    ax.set_xlabel("Avaliação (0–5)")
    ax.set_ylabel("Preço (R$)")
    ax.set_title("Preço × Avaliação (0–5) — Magalu x KaBuM!")
    ax.legend(title="")
    out = os.path.join(OUTPUT_DIR, "03_price_vs_rating.png")
    savefig(out)
    return out

def var_pct_line(df):
    d = (
        df.groupby(["fornecedor", "date"], as_index=False)["price"]
          .mean()
          .sort_values(["fornecedor", "date"])
    )
    if d.empty:
        return None
    d["pct_change"] = d.groupby("fornecedor")["price"].pct_change() * 100.0

    plt.figure(figsize=(9, 6))
    ax = sns.lineplot(
        data=d, x="date", y="pct_change",
        hue="fornecedor", palette=PALETTE, marker="o"
    )
    for fornecedor, g in d.groupby("fornecedor"):
        g = g.dropna(subset=["pct_change"])
        if len(g) > 1:
            plt.fill_between(
                pd.to_datetime(g["date"]),
                g["pct_change"],
                alpha=0.12,
                color=PALETTE.get(fornecedor, "#999999"),
            )
    ax.axhline(0, color="gray", linestyle="--", linewidth=1)
    ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=4, maxticks=8))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("Variação diária (%)")
    plt.xlabel("Data")
    plt.title("Variação percentual diária do preço médio — Magalu x KaBuM!")
    plt.legend(title="")
    out = os.path.join(OUTPUT_DIR, "04_var_pct_line.png")
    savefig(out)
    return out

def total_price_pie(df):
    d = (
        df.groupby("fornecedor", as_index=False)["total_price"]
          .mean()
          .loc[lambda x: x["fornecedor"].isin(["Magalu", "KaBuM!"])]
          .sort_values("total_price")
    )
    if d.empty:
        return None
    plt.figure(figsize=(7, 7))
    colors = [PALETTE.get(f, "#999999") for f in d["fornecedor"]]
    wedges, texts, autotexts = plt.pie(
        d["total_price"], labels=d["fornecedor"],
        autopct="%1.1f%%", colors=colors,
        startangle=90, counterclock=False, pctdistance=0.8
    )
    centre_circle = plt.Circle((0, 0), 0.55, fc="white")
    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)
    plt.title("Proporção do preço total médio — Magalu x KaBuM!")
    out = os.path.join(OUTPUT_DIR, "05_total_price_pie.png")
    savefig(out)
    return out

# ============== Rankings (com filtro aplicado) ==============
def export_rankings(df):
    last_date = df.groupby("id_product")["date"].max().reset_index().rename(columns={"date": "last_date"})
    d = df.merge(last_date, on="id_product", how="left")
    d = d[d["date"] == d["last_date"]]

    p_rank = (
        d.groupby(["id_product", "brand", "model", "product_code"], as_index=False)["price"]
         .mean()
         .sort_values("price", ascending=True)
         .head(5)
    )
    path_price = os.path.join(OUTPUT_DIR, "rank_top5_mais_baratos.csv")
    p_rank.to_csv(path_price, index=False)

    r = d.dropna(subset=["avaliacao"])
    r_rank = (
        r.groupby(["id_product", "brand", "model", "product_code"], as_index=False)["avaliacao"]
         .mean()
         .sort_values("avaliacao", ascending=False)
         .head(5)
    )
    path_rating = os.path.join(OUTPUT_DIR, "rank_top5_melhor_avaliados.csv")
    r_rank.to_csv(path_rating, index=False)

    return path_price, path_rating

# ============== CLI/MAIN ==============
def parse_args():
    ap = argparse.ArgumentParser(description="Gráficos (apenas Magalu e KaBuM!) do MySQL de scraping.")
    ap.add_argument("--like", dest="product_like", default=None, help="Filtro por substring (marca/modelo), ex.: 'iPhone 15'.")
    ap.add_argument("--code", dest="product_code", default=None, help="Filtro por código exato do produto (Products.code).")
    return ap.parse_args()

def main():
    args = parse_args()
    df = load_denormalized_only_main()

    # filtros (opcionais)
    if args.product_code or args.product_like:
        df = filter_product(df, args.product_code, args.product_like)

    print(f"[INFO] Linhas após filtro: {len(df)}")
    if df.empty:
        print("[AVISO] DataFrame vazio. Nada a plotar.")
        return

    outs = [
        price_trend(df),
        store_competition(df),
        price_vs_rating(df),
        var_pct_line(df),
        total_price_pie(df),
    ]
    csv_price, csv_rating = export_rankings(df)

    print("[SAÍDAS]")
    for o in outs:
        if o:
            print(" ", o)
    print(" ", csv_price)
    print(" ", csv_rating)

if __name__ == "__main__":
    main()
