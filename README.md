# 🕸️ Web Scraper de Marketplaces – Python

Este projeto tem como objetivo **mapear e coletar informações de produtos em marketplaces online**, incluindo **preços**, **avaliações** e **valores de entrega**.  
O scraper foi desenvolvido em **Python**, utilizando **Selenium** e **BeautifulSoup**, e os dados coletados são tratados e exportados para planilhas através do **Pandas**.

---

## 🎯 Objetivo

Automatizar a **coleta e análise de dados de e-commerce**, permitindo comparar produtos entre diferentes marketplaces e gerar insights sobre:
- 💰 Variação de preços  
- ⭐ Avaliações médias  
- 🚚 Custos e prazos de entrega  

---

## ⚙️ Funcionalidades

- 🔎 Coleta automática de informações de produtos  
- 🏷️ Extração de preço, nome, avaliação e valor de frete  
- 🕓 Esperas dinâmicas com Selenium para páginas com JavaScript  
- 💾 Armazenamento em planilhas `.csv` ou `.xlsx`  
- 🧹 Tratamento e limpeza de dados com Pandas  
- 🔁 Execução programável (para coleta periódica)

---

## 🧠 Tecnologias Utilizadas

| Tecnologia | Função |
|-------------|--------|
| **Python 3.10+** | Linguagem principal |
| **Selenium** | Navegação automatizada e scraping dinâmico |
| **BeautifulSoup (bs4)** | Extração e parsing de HTML |
| **Pandas** | Estruturação e exportação dos dados |
| **WebDriver (Chrome/Firefox)** | Automação do navegador |
| **time / re / os / json** | Utilitários internos para controle e tratamento |

---

---

## 🧩 Exemplo de Dados Coletados

| Produto | Preço | Avaliação | Frete | Marketplace |
|----------|--------|------------|--------|--------------|
| Fone Bluetooth XYZ | R$ 89,90 | ⭐ 4.6 | Grátis | Amazon |
| Teclado Mecânico ABC | R$ 219,00 | ⭐ 4.8 | R$ 15,00 | Mercado Livre |
| Monitor 24” UltraWide | R$ 799,90 | ⭐ 4.5 | R$ 25,00 | Magazine Luiza |

---

## 💻 Como Executar o Projeto

### 🔹 1. Clonar o repositório
```bash
git clone https://github.com/seu-usuario/webscraper-marketplaces.git
cd webscraper-marketplaces

3. Instalar dependências
pip install -r requirements.txt

4- Executar o scraper
python scraper.py
´´´
