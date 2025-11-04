#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gera 1 gráfico por produto (Magalu x KaBuM!) mostrando a variação temporal do preço
E também gera um **gráfico único** com os **Top 5 produtos** (small multiples).

Uso exemplos:
  python resultados_produtos_top5.py --like "Galaxy S24" --max-products 10
  python resultados_produtos_top5.py --min-days 2
"""

import os
import argparse
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sqlalchemy import create_engine
import re
from math import ceil

# ============== CONFIG (env com fallback) ==============
MYSQL_USER = os.environ.get("MYSQL_USER", "root")
MYSQL_PASS = os.environ.get("MYSQL_PASS", "admin")
MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = os.environ.get("MYSQL_PORT", "3306")
MYSQL_DB   = os.environ.get("MYSQL_DB", "ecommerce_scraping")

OUTPUT_DIR = os.environ.get("SCRAPE_OUTPUT_DIR", "./outputs")
OUT_PROD_DIR = os.path.join(OUTPUT_DIR, "products")
os.makedirs(OUT_PROD_DIR, exist_ok=True)

# Paleta fixa Magalu/KaBuM!
PALETTE = {"Magalu": "#1f77b4", "KaBuM!": "#ff7f0e"}

sns.set_theme(context="notebook", style="whitegrid")

# ============== Helpers ==============
def _norm(s: str) -> str:
    return (s or "").strip().lower()

def normalize_fornecedor(name: str) -> str:
    n = _norm(name)
    if not n:
        return "Desconhecido"
    if "magalu" in n or "magazineluiza" in n:
        return "Magalu"
    if "kabum" in n:
        return "KaBuM!"
    return (name or "").strip() or "Desconhecido"

def slugify(s: str, max_len: int = 80) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s_-]+", "-", s).strip("-")
    return s[:max_len] if s else "produto"

# ============== Conexão e carga ==============
def get_engine():
    uri = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
    return create_engine(uri, pool_pre_ping=True)

def load_denormalized_only_main():
    """Lê do MySQL, normaliza e mantém apenas Magalu/KaBuM!"""
    eng = get_engine()
    sql = """
    SELECT
        L.id_list, L.url, L.price, L.avaliacao, L.frete_price, L.prazo_entrega, L.created_at,
        F.name AS fornecedor,
        S.name AS seller,
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

    df["fornecedor"] = df["fornecedor"].astype(str).map(normalize_fornecedor)
    df = df[df["fornecedor"].isin(["Magalu", "KaBuM!"])].copy()

    df.loc[~df["avaliacao"].between(0, 5, inclusive="both"), "avaliacao"] = np.nan
    df["total_price"] = df["price"].fillna(0) + df["frete_price"].fillna(0)
    return df

# ============== Filtros ==============
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

# ============== Plot por produto ==============
def plot_product_timeline(df, pid, brand, model, code, min_days=2):
    """Gera um único gráfico (linha) para o produto, com preço médio diário por fornecedor."""
    d = (
        df[df["id_product"] == pid]
        .groupby(["fornecedor", "date"], as_index=False)["price"].mean()
        .sort_values(["fornecedor", "date"])
    )
    if d["date"].nunique() < min_days:
        return None

    plt.figure(figsize=(10, 6))
    ax = sns.lineplot(data=d, x="date", y="price", hue="fornecedor", palette=PALETTE, marker="o")
    ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=4, maxticks=8))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
    plt.xticks(rotation=45, ha="right")

    title_brand = (brand or "").strip(); title_model = (model or "").strip()
    title = f"{title_brand} {title_model}".strip() or "Produto"
    plt.title(f"Variação temporal do preço — {title}")
    plt.xlabel("Data"); plt.ylabel("Preço médio (R$)"); plt.legend(title=""); plt.tight_layout()

    slug = slugify(f"{title_brand}-{title_model}-{code}")
    out_path = os.path.join(OUT_PROD_DIR, f"{slug}.png")
    plt.savefig(out_path, dpi=150, bbox_inches="tight"); plt.close()
    return out_path

def plot_all_products(df, max_products=None, min_days=2):
    outs = []
    prods = (
        df[["id_product", "brand", "model", "product_code"]]
        .drop_duplicates()
        .sort_values(["brand", "model", "product_code"])
    )
    if max_products is not None:
        prods = prods.head(int(max_products))

    for _, r in prods.iterrows():
        out = plot_product_timeline(df, r["id_product"], r["brand"], r["model"], r["product_code"], min_days=min_days)
        if out: outs.append(out)
    return outs

# ============== Top 5 em um único gráfico (grid) ==============
def pick_top5_products(df, min_days=2):
    coverage = (
        df.groupby("id_product")["date"].nunique().reset_index(name="n_dates").sort_values("n_dates", ascending=False)
    )
    coverage = coverage[coverage["n_dates"] >= min_days]
    top = coverage.head(5)["id_product"].tolist()
    meta = (
        df[df["id_product"].isin(top)][["id_product","brand","model","product_code"]].drop_duplicates()
    )
    return top, {row["id_product"]:(row["brand"], row["model"], row["product_code"]) for _, row in meta.iterrows()}

def plot_top5_products_grid(df, min_days=2):
    ids, meta = pick_top5_products(df, min_days=min_days)
    if not ids: return None

    n = len(ids); rows = (n + 2) // 3; cols = min(3, n)
    fig, axes = plt.subplots(rows, cols, figsize=(5*cols, 3.8*rows), squeeze=False, sharex=False, sharey=False)
    axes = axes.flatten()

    for i, pid in enumerate(ids):
        ax = axes[i]
        d = (
            df[df["id_product"] == pid]
            .groupby(["fornecedor", "date"], as_index=False)["price"].mean()
            .sort_values(["fornecedor", "date"])
        )
        brand, model, code = meta.get(pid, ("", "", ""))
        title = f"{(brand or '').strip()} {(model or '').strip()}".strip() or "Produto"

        sns.lineplot(data=d, x="date", y="price", hue="fornecedor", palette=PALETTE, marker="o", ax=ax, legend=False)
        ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=3, maxticks=6))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
        ax.set_title(title, fontsize=11); ax.set_xlabel(""); ax.set_ylabel("R$")

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, ["Magalu", "KaBuM!"], loc="upper center", ncol=2, frameon=False)
    fig.suptitle("Top 5 produtos — variação temporal do preço (Magalu x KaBuM!)", fontsize=14, y=0.98)
    fig.tight_layout(rect=[0, 0, 1, 0.95])

    out_path = os.path.join(OUTPUT_DIR, "top5_produtos_timeline.png")
    fig.savefig(out_path, dpi=150, bbox_inches="tight"); plt.close(fig)
    return out_path

# ============== CLI/MAIN ==============
def parse_args():
    ap = argparse.ArgumentParser(description="Gráficos por produto (Magalu x KaBuM!) + Top 5 em grid.")
    ap.add_argument("--like", dest="product_like", default=None, help="Filtro por substring (marca/modelo), ex.: 'iPhone 15'.")
    ap.add_argument("--code", dest="product_code", default=None, help="Filtro por código exato do produto (Products.code).")
    ap.add_argument("--max-products", type=int, default=None, help="Limita a quantidade de produtos a plotar individualmente.")
    ap.add_argument("--min-days", type=int, default=2, help="Mínimo de datas para desenhar timelines (padrão=2).")
    return ap.parse_args()

def main():
    args = parse_args()
    df = load_denormalized_only_main()
    if args.product_code or args.product_like:
        df = filter_product(df, args.product_code, args.product_like)

    print(f"[INFO] Linhas após filtro: {len(df)}")
    if df.empty:
        print("[AVISO] DataFrame vazio. Nada a plotar."); return

    grid_png = plot_top5_products_grid(df, min_days=args.min_days)
    if grid_png: print("[Top 5] ", grid_png)
    else: print("[Top 5] Sem produtos com dias suficientes.")

    outs = plot_all_products(df, max_products=args.max_products, min_days=args.min_days)
    print("[Individuais]")
    if outs:
        for o in outs: print(" ", o)
    else:
        print("  Nenhum gráfico individual gerado.")

if __name__ == "__main__":
    main()
