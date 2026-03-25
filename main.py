import sqlite3
import requests
import json
import time
import pandas as pd
import os
import base64
import re
from datetime import datetime

# Importações do Sniper VivaReal
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

hoje = datetime.now().strftime('%Y-%m-%d')
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

print("🚀 INICIANDO SUPER SISTEMA (QUINTOANDAR + VIVAREAL)...")

pasta_fotos = 'fotos_imoveis'
if not os.path.exists(pasta_fotos): os.makedirs(pasta_fotos)

conn = sqlite3.connect('monitor_imoveis.db')
cursor = conn.cursor()

cursor.execute('''CREATE TABLE IF NOT EXISTS imoveis (id_imovel TEXT PRIMARY KEY, tipo TEXT, cidade TEXT, bairro TEXT, rua TEXT, cep TEXT, area_m2 REAL, quartos INTEGER, banheiros INTEGER, vagas INTEGER, preco_venda REAL, preco_aluguel REAL, condominio REAL, iptu REAL, data_primeira_vista TEXT, data_ultima_vista TEXT, status TEXT, data_publicacao TEXT, data_pub_venda TEXT, data_pub_aluguel TEXT, origem TEXT)''')
cursor.execute('''CREATE TABLE IF NOT EXISTS historico_precos (id_imovel TEXT, data_alteracao TEXT, mercado TEXT, preco_antigo REAL, preco_novo REAL)''')
try: cursor.execute("ALTER TABLE imoveis ADD COLUMN origem TEXT")
except: pass
conn.commit()

cursor.execute("SELECT id_imovel, preco_venda, preco_aluguel FROM imoveis")
memoria_precos = {row[0]: {'venda': row[1], 'aluguel': row[2]} for row in cursor.fetchall()}

# ==========================================
# 1. MOTOR QUINTOANDAR (API RÁPIDA)
# ==========================================
print("\n🚁 Iniciando Mapeamento QuintoAndar...")
url_mapa = "https://apigw.prod.quintoandar.com.br/house-listing-search/v2/search/coordinates"
params_base = {"context.mapShowing": "true", "context.listShowing": "true", "filters.location.coordinate.lat": "-23.5255", "filters.location.coordinate.lng": "-46.7733", "filters.location.viewport.east": "-46.7600", "filters.location.viewport.north": "-23.5150", "filters.location.viewport.south": "-23.5350", "filters.location.viewport.west": "-46.7850", "filters.location.countryCode": "BR", "filters.houseSpecs.houseTypes[0]": "APARTMENT"}

lista_ids_qa = []
try:
    params_venda = params_base.copy(); params_venda["filters.businessContext"] = "SALE"
    ids_venda = [item['_id'] for item in requests.get(url_mapa, params=params_venda, headers=headers).json().get('hits', {}).get('hits', [])]
    params_aluguel = params_base.copy(); params_aluguel["filters.businessContext"] = "RENT"
    ids_aluguel = [item['_id'] for item in requests.get(url_mapa, params=params_aluguel, headers=headers).json().get('hits', {}).get('hits', [])]
    lista_ids_qa = list(set(ids_venda + ids_aluguel))
except: pass

def checar_mercado_qa(id_imovel, mercado):
    try:
        res = requests.get(f"https://www.quintoandar.com.br/imovel/{id_imovel}/{mercado}", headers=headers)
        if '<script id="__NEXT_DATA__"' in res.text:
            json_str = res.text.split('<script id="__NEXT_DATA__" type="application/json">')[1].split('</script>')[0]
            house_info = json.loads(json_str)['props']['pageProps']['initialState']['house']['houseInfo']
            return house_info.get('status', '').lower() in ['publicado', 'published'], house_info.get('salePrice') if mercado == 'comprar' else house_info.get('rentPrice') or 0
    except: pass
    return False, 0

for id_imovel in lista_ids_qa:
    try:
        res_base = requests.get(f"https://www.quintoandar.com.br/imovel/{id_imovel}", headers=headers)
        if '06216-160' in res_base.text or '06216160' in res_base.text:
            json_str = res_base.text.split('<script id="__NEXT_DATA__" type="application/json">')[1].split('</script>')[0]
            house_info = json.loads(json_str)['props']['pageProps']['initialState']['house']['houseInfo']
            area = house_info.get('area') or 0
            p_v_base = house_info.get('salePrice') or 0
            
            if area <= 40 and (p_v_base <= 420000 or p_v_base == 0):
                v_ativa, p_venda = checar_mercado_qa(id_imovel, 'comprar')
                a_ativa, p_alug = checar_mercado_qa(id_imovel, 'alugar')
                
                status_real = 'Venda e Aluguel' if v_ativa and a_ativa else 'Apenas Venda' if v_ativa else 'Apenas Aluguel' if a_ativa else 'Indisponível'
                
                cursor.execute("SELECT id_imovel FROM imoveis WHERE id_imovel = ?", (id_imovel,))
                if cursor.fetchone() is None:
                    cursor.execute('''INSERT INTO imoveis (id_imovel, tipo, cidade, bairro, rua, cep, area_m2, quartos, banheiros, vagas, preco_venda, preco_aluguel, condominio, iptu, data_primeira_vista, data_ultima_vista, status, origem) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (id_imovel, 'Apartamento', 'Osasco', 'Presidente Altino', "Doutor Jubair Celestino", '06216-160', area, house_info.get('bedrooms'), house_info.get('bathrooms'), house_info.get('parkingSpaces'), p_venda, p_alug, house_info.get('condoPrice'), house_info.get('iptu'), hoje, hoje, status_real, 'QuintoAndar'))
                else:
                    cursor.execute('''UPDATE imoveis SET data_ultima_vista = ?, status = ?, preco_venda = ?, preco_aluguel = ? WHERE id_imovel = ?''', (hoje, status_real, p_venda, p_alug, id_imovel))
                conn.commit()
    except: pass

# ==========================================
# 2. MOTOR VIVAREAL (SNIPER INVISÍVEL)
# ==========================================
print("\n🕵️ Iniciando Infiltração VivaReal...")
chrome_options = Options()
chrome_options.add_argument('--headless=new') 
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

rotas_vr = {
    "Venda": "https://www.vivareal.com.br/venda/sp/osasco/bairros/presidente-altino/rua-jubair-celestino/apartamento_residencial/",
    "Aluguel": "https://www.vivareal.com.br/aluguel/sp/osasco/bairros/presidente-altino/rua-jubair-celestino/apartamento_residencial/"
}

fotos_pendentes_vr = {} # Para baixar depois

for mercado, url_base in rotas_vr.items():
    pagina = 1
    while True:
        driver.get(f"{url_base}?pagina={pagina}")
        for i in range(1, 11):
            driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * ({i}/10));")
            time.sleep(0.5) 
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        links_imoveis = soup.find_all('a', href=re.compile(r'/imovel/'))
        if len(links_imoveis) < 10: break

        for link_tag in links_imoveis:
            try:
                url = link_tag.get('href', '')
                if not url or "ERROR" in url or "showcase" in url.lower() or "osasco" not in url.lower(): continue
                
                card = link_tag.find_parent('article') or link_tag.parent.parent.parent.parent.parent.parent
                texto_card = card.get_text(separator=' ', strip=True).lower()
                
                match_id = re.search(r'-id-(\d+)', url)
                match_area = re.search(r'-(\d+)m2-', url)
                match_preco = re.search(r'-RS(\d+)-id', url)
                
                if match_area and match_preco and match_id:
                    id_imovel = f"VR-{match_id.group(1)}"
                    area = int(match_area.group(1))
                    preco = int(match_preco.group(1))
                    
                    if 0 < area <= 40 and (mercado == "Aluguel" or preco <= 420000):
                        q_url = re.search(r'-(\d+)-quarto', url)
                        quartos = int(q_url.group(1)) if q_url else int((re.search(r'(\d+)\s*quarto', texto_card) or type('obj', (object,), {'group': lambda self, x: 0})()).group(1))
                        vagas = int((re.search(r'(\d+)\s*vaga', texto_card) or type('obj', (object,), {'group': lambda self, x: 0})()).group(1))
                        banheiros = int((re.search(r'(\d+)\s*banheiro', texto_card) or type('obj', (object,), {'group': lambda self, x: 0})()).group(1))
                        
                        fotos = list(dict.fromkeys([img.get('src') or img.get('data-src') or '' for img in card.find_all('img') if 'http' in (img.get('src') or img.get('data-src') or '')]))[:3]
                        if fotos: fotos_pendentes_vr[id_imovel] = fotos
                        
                        p_venda = preco if mercado == "Venda" else 0
                        p_alug = preco if mercado == "Aluguel" else 0
                        status_real = f"Apenas {mercado}"

                        cursor.execute("SELECT id_imovel FROM imoveis WHERE id_imovel = ?", (id_imovel,))
                        if cursor.fetchone() is None:
                            cursor.execute('''INSERT INTO imoveis (id_imovel, tipo, cidade, bairro, rua, cep, area_m2, quartos, banheiros, vagas, preco_venda, preco_aluguel, condominio, iptu, data_primeira_vista, data_ultima_vista, status, origem) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (id_imovel, 'Apartamento', 'Osasco', 'Presidente Altino', "Doutor Jubair Celestino", '06216-160', area, quartos, banheiros, vagas, p_venda, p_alug, 0, 0, hoje, hoje, status_real, 'VivaReal'))
                        else:
                            cursor.execute('''UPDATE imoveis SET data_ultima_vista = ?, status = ? WHERE id_imovel = ?''', (hoje, status_real, id_imovel))
                            if mercado == "Venda": cursor.execute("UPDATE imoveis SET preco_venda = ? WHERE id_imovel = ?", (preco, id_imovel))
                            if mercado == "Aluguel": cursor.execute("UPDATE imoveis SET preco_aluguel = ? WHERE id_imovel = ?", (preco, id_imovel))
                        conn.commit()
            except: pass
        pagina += 1
        if pagina > 5: break
driver.quit()

# ==========================================
# 3. DOWNLOAD DE FOTOS (AMBOS) E HTML
# ==========================================
print("\n📸 Baixando imagens faltantes...")
cursor.execute("SELECT id_imovel, origem FROM imoveis")
for (id_imovel, origem) in cursor.fetchall():
    if not os.path.exists(f"{pasta_fotos}/{id_imovel}_foto_1.jpg"):
        if origem == 'QuintoAndar':
            try:
                res = requests.get(f"https://www.quintoandar.com.br/imovel/{id_imovel}", headers=headers)
                json_str = res.text.split('<script id="__NEXT_DATA__" type="application/json">')[1].split('</script>')[0]
                nomes = list(dict.fromkeys(re.findall(r'([a-zA-Z0-9_.-]*' + str(id_imovel) + r'[a-zA-Z0-9_.-]*\.(?:jpg|jpeg|webp))', json_str) or re.findall(r'(original[a-zA-Z0-9_.-]+\.(?:jpg|jpeg|webp))', json_str)))[:3]
                for i, nome in enumerate(nomes):
                    img_data = requests.get(f"https://www.quintoandar.com.br/img/800x600/{nome}", headers=headers).content
                    with open(f"{pasta_fotos}/{id_imovel}_foto_{i+1}.jpg", 'wb') as f: f.write(img_data)
            except: pass
        elif origem == 'VivaReal' and id_imovel in fotos_pendentes_vr:
            for i, url_foto in enumerate(fotos_pendentes_vr[id_imovel]):
                try:
                    img_data = requests.get(url_foto, headers=headers).content
                    with open(f"{pasta_fotos}/{id_imovel}_foto_{i+1}.jpg", 'wb') as f: f.write(img_data)
                except: pass

print("🖥️ Gerando Dashboard Unificado...")
df_imoveis = pd.read_sql_query("SELECT * FROM imoveis", conn)
df_historico = pd.read_sql_query("SELECT * FROM historico_precos", conn)
conn.close()

html = """<!DOCTYPE html><html lang="pt-BR"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Dashboard Imobiliário</title><style>body { margin: 0; padding: 0; background-color: #e9ecef; } .dashboard-header { background: #2c3e50; color: white; padding: 20px; font-family: sans-serif; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 15px;} .botoes-topo { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; } .botoes-topo a { background-color: #2ecc71; color: white; padding: 8px 15px; border-radius: 6px; text-decoration: none; font-size: 14px; font-weight: bold; } .botoes-topo a.btn-hist { background-color: #f39c12; } .control-group { display: flex; gap: 10px; flex-wrap: wrap; } .control-group select { padding: 10px; border-radius: 6px; border: none; font-weight: bold; } .vitrine-container { display: flex; flex-wrap: wrap; gap: 20px; font-family: sans-serif; padding: 20px; justify-content: center; } .card-imovel { background: white; border-radius: 12px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); width: 340px; overflow: hidden; display: flex; flex-direction: column; position: relative; } .galeria-fotos { display: flex; overflow-x: auto; scroll-snap-type: x mandatory; } .galeria-fotos img { width: 100%; height: 200px; object-fit: cover; scroll-snap-align: center; flex-shrink: 0; } .galeria-fotos::-webkit-scrollbar { display: none; } .info-imovel { padding: 15px; display: flex; flex-direction: column; gap: 8px; height: 100%; } .preco-box { background: #f8f9fa; padding: 12px; border-radius: 8px; border: 1px solid #dee2e6; } .linha-mercado { display: flex; justify-content: space-between; align-items: center; margin-bottom: 5px; } .preco-venda { font-size: 18px; font-weight: bold; color: #2ecc71; margin: 0; } .preco-aluguel { font-size: 16px; font-weight: bold; color: #f39c12; margin: 0; } .preco-indisponivel { color: #95a5a6; text-decoration: line-through; } .mini-badge { padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; color: white; text-transform: uppercase; } .badge-on { background-color: #2ecc71; } .badge-off { background-color: #e74c3c; } .historico-box { background: #fff3cd; color: #856404; font-size: 11px; padding: 8px; border-radius: 6px; margin-top: 5px; } .tags { display: flex; gap: 10px; font-size: 13px; font-weight: bold; color: #34495e; background: #ecf0f1; padding: 8px; border-radius: 6px; justify-content: space-around; } .endereco { font-size: 13px; color: #95a5a6; margin-bottom: 5px; } .datas-info { font-size: 11px; color: #95a5a6; text-align: right; } .container-botoes { display: flex; gap: 10px; margin-top: auto; } .botao-link { text-align: center; color: white; text-decoration: none; padding: 10px; border-radius: 6px; font-weight: bold; font-size: 13px; flex: 1; } .botao-venda { background: #2ecc71; } .botao-aluguel { background: #f39c12; } .selo-origem { position: absolute; top: 10px; right: 10px; color: white; padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: bold; z-index: 10; box-shadow: 0 2px 4px rgba(0,0,0,0.3); } .origem-qa { background: #3498db; } .origem-vr { background: #16a085; } .selo-alerta { position: absolute; top: 10px; left: 10px; color: white; padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: bold; z-index: 10; box-shadow: 0 2px 4px rgba(0,0,0,0.3); text-transform: uppercase; } .selo-novo { background: #9b59b6; } .selo-alterado { background: #e67e22; }</style></head><body>
<div class="dashboard-header">
    <div class="botoes-topo"><h2>🏢 Dashboard Unificado</h2><a href="https://github.com/GustavoYoshiharuMiyake/dashboard-imoveis/actions" target="_blank">🔄 Forçar Atualização</a><a href="historico.html" class="btn-hist">📖 Ver Histórico</a></div>
    <div class="control-group">
        <select id="filtroStatus" onchange="aplicarFiltros()"><option value="Todos">👁️ Todos os Imóveis</option><option value="Apenas Venda">💰 Filtro: Venda Ativa</option><option value="Apenas Aluguel">🔑 Filtro: Aluguel Ativo</option></select>
        <select id="sortOrder" onchange="aplicarFiltros()"><option value="default">↕️ Ordenação Padrão</option><option value="vendaAsc">📈 Menor Preço (Venda)</option><option value="vendaDesc">📉 Maior Preço (Venda)</option><option value="aluguelAsc">📈 Menor Preço (Aluguel)</option></select>
    </div>
</div><div class='vitrine-container' id="containerCards">
"""

for _, row in df_imoveis.iterrows():
    id_imovel, status, origem = row['id_imovel'], row.get('status', 'Indisponível'), row.get('origem', 'QuintoAndar')
    val_v, val_a = row.get('preco_venda') or 0, row.get('preco_aluguel') or 0
    html_origem = f"<div class='selo-origem origem-qa'>QuintoAndar</div>" if origem == 'QuintoAndar' else f"<div class='selo-origem origem-vr'>VivaReal</div>"
    selo_html = "<div class='selo-alerta selo-novo'>✨ Novo</div>" if row.get('data_primeira_vista') == hoje else ""
    
    html_fotos = ""
    for i in range(1, 4):
        caminho_foto = f"fotos_imoveis/{id_imovel}_foto_{i}.jpg"
        if os.path.exists(caminho_foto):
            with open(caminho_foto, "rb") as img_file:
                html_fotos += f"<img src='data:image/*;base64,{base64.b64encode(img_file.read()).decode('utf-8')}'>"
    if not html_fotos: html_fotos = "<div style='height:200px; display:flex; align-items:center; justify-content:center; background:#bdc3c7; color:white;'>Sem fotos</div>"

    txt_v = f"R$ {val_v:,.0f}".replace(',', '.') if val_v > 0 else "N/A"
    txt_a = f"R$ {val_a:,.0f}".replace(',', '.') if val_a > 0 else "N/A"
    
    cv, bv, tv = ("preco-venda", "badge-on", "ON") if status in ['Apenas Venda', 'Venda e Aluguel'] else ("preco-venda preco-indisponivel", "badge-off", "OFF")
    ca, ba, ta = ("preco-aluguel", "badge-on", "ON") if status in ['Apenas Aluguel', 'Venda e Aluguel'] else ("preco-aluguel preco-indisponivel", "badge-off", "OFF")

    link_v = f"https://www.quintoandar.com.br/imovel/{id_imovel}/comprar" if origem == 'QuintoAndar' else f"https://www.vivareal.com.br/imovel/venda-id-{id_imovel.replace('VR-','')}"
    link_a = f"https://www.quintoandar.com.br/imovel/{id_imovel}/alugar" if origem == 'QuintoAndar' else f"https://www.vivareal.com.br/imovel/aluguel-id-{id_imovel.replace('VR-','')}"
    
    html_botoes = "<div class='container-botoes'>"
    if 'Venda' in status: html_botoes += f"<a href='{link_v}' target='_blank' class='botao-link botao-venda'>Ver Venda</a>"
    if 'Aluguel' in status: html_botoes += f"<a href='{link_a}' target='_blank' class='botao-link botao-aluguel'>Ver Aluguel</a>"
    html_botoes += "</div>"
    
    html += f"""
    <div class='card-imovel' data-status='{status}' data-venda='{val_v}' data-aluguel='{val_a}'>
        {html_origem} {selo_html} <div class='galeria-fotos'>{html_fotos}</div>
        <div class='info-imovel'>
            <div class='preco-box'>
                <div class='linha-mercado'><p class='{cv}'>Venda: {txt_v}</p><span class='mini-badge {bv}'>{tv}</span></div>
                <div class='linha-mercado'><p class='{ca}'>Alug: {txt_a}</p><span class='mini-badge {ba}'>{ta}</span></div>
            </div>
            <div class='tags'><span>📏 {row['area_m2']}m²</span><span>🛏️ {row['quartos']} Qt</span><span>🚗 {row['vagas']} Vg</span></div>
            <p class='endereco'>📍 {row['rua']} - {row['bairro']}</p><div class='datas-info'>Atualizado: {row['data_ultima_vista']}</div>
            {html_botoes}
        </div>
    </div>"""

html += """</div><script>
function aplicarFiltros() {
    let f = document.getElementById('filtroStatus').value, s = document.getElementById('sortOrder').value;
    let container = document.getElementById('containerCards'); let cards = Array.from(document.querySelectorAll('.card-imovel'));
    cards.forEach(c => { let st = c.getAttribute('data-status'); c.style.display = (f === 'Todos' || (f === 'Apenas Venda' && st.includes('Venda')) || (f === 'Apenas Aluguel' && st.includes('Aluguel'))) ? 'flex' : 'none'; });
    cards.sort((a, b) => {
        let va = parseFloat(a.getAttribute('data-venda')) || Infinity, vb = parseFloat(b.getAttribute('data-venda')) || Infinity;
        let aa = parseFloat(a.getAttribute('data-aluguel')) || Infinity, ab = parseFloat(b.getAttribute('data-aluguel')) || Infinity;
        if (s === 'vendaAsc') return va - vb; if (s === 'vendaDesc') return (vb===Infinity?-Infinity:vb) - (va===Infinity?-Infinity:va);
        if (s === 'aluguelAsc') return aa - ab; return 0;
    });
    cards.forEach(c => container.appendChild(c));
}</script></body></html>"""

with open("index.html", "w", encoding="utf-8") as f: f.write(html)
print("✅ Tudo pronto! Dashboard atualizado na nuvem.")
