import sqlite3
import requests
import json
import time
import pandas as pd
import os
import base64
import re
from datetime import datetime

hoje = datetime.now().strftime('%Y-%m-%d')
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

print("🚀 INICIANDO SISTEMA MESTRE NA NUVEM...")

pasta_fotos = 'fotos_imoveis'
if not os.path.exists(pasta_fotos): os.makedirs(pasta_fotos)

conn = sqlite3.connect('monitor_imoveis.db')
cursor = conn.cursor()

cursor.execute('''CREATE TABLE IF NOT EXISTS imoveis (id_imovel TEXT PRIMARY KEY, tipo TEXT, cidade TEXT, bairro TEXT, rua TEXT, cep TEXT, area_m2 REAL, quartos INTEGER, banheiros INTEGER, vagas INTEGER, preco_venda REAL, preco_aluguel REAL, condominio REAL, iptu REAL, data_primeira_vista TEXT, data_ultima_vista TEXT, status TEXT, data_publicacao TEXT, data_pub_venda TEXT, data_pub_aluguel TEXT)''')
cursor.execute('''CREATE TABLE IF NOT EXISTS historico_precos (id_imovel TEXT, data_alteracao TEXT, mercado TEXT, preco_antigo REAL, preco_novo REAL)''')
try: cursor.execute("ALTER TABLE imoveis ADD COLUMN data_pub_venda TEXT")
except: pass
try: cursor.execute("ALTER TABLE imoveis ADD COLUMN data_pub_aluguel TEXT")
except: pass
conn.commit()

cursor.execute("SELECT id_imovel, preco_venda, preco_aluguel FROM imoveis")
memoria_precos = {row[0]: {'venda': row[1], 'aluguel': row[2]} for row in cursor.fetchall()}

url_mapa = "https://apigw.prod.quintoandar.com.br/house-listing-search/v2/search/coordinates"
params_mapa = {"context.mapShowing": "true", "context.listShowing": "true", "filters.businessContext": "SALE", "filters.location.coordinate.lat": "-23.5255", "filters.location.coordinate.lng": "-46.7733", "filters.location.viewport.east": "-46.7600", "filters.location.viewport.north": "-23.5150", "filters.location.viewport.south": "-23.5350", "filters.location.viewport.west": "-46.7850", "filters.location.countryCode": "BR", "filters.houseSpecs.houseTypes[0]": "APARTMENT"}

print("🚁 Mapeando Presidente Altino...")
try:
    res_mapa = requests.get(url_mapa, params=params_mapa, headers=headers)
    lista_ids = [item['_id'] for item in res_mapa.json().get('hits', {}).get('hits', [])]
except:
    lista_ids = []

def checar_mercado(id_imovel, mercado):
    try:
        res = requests.get(f"https://www.quintoandar.com.br/imovel/{id_imovel}/{mercado}", headers=headers)
        match_data = re.search(r'data-testid="publication_date"[^>]*>.*?<span>(Publicado[^<]+)</span>', res.text)
        data_front = match_data.group(1).replace("Publicado ", "") if match_data else "Sem registro"
        if '<script id="__NEXT_DATA__"' in res.text:
            json_str = res.text.split('<script id="__NEXT_DATA__" type="application/json">')[1].split('</script>')[0]
            house_info = json.loads(json_str)['props']['pageProps']['initialState']['house']['houseInfo']
            is_ativo = house_info.get('status', '').lower() in ['publicado', 'published']
            preco = house_info.get('salePrice') if mercado == 'comprar' else house_info.get('rentPrice')
            return is_ativo, preco or 0, data_front
    except: pass
    return False, 0, "Indisponível"

for id_imovel in lista_ids:
    try:
        res_base = requests.get(f"https://www.quintoandar.com.br/imovel/{id_imovel}", headers=headers)
        if '06216-160' in res_base.text or '06216160' in res_base.text:
            json_str = res_base.text.split('<script id="__NEXT_DATA__" type="application/json">')[1].split('</script>')[0]
            house_info = json.loads(json_str)['props']['pageProps']['initialState']['house']['houseInfo']
            area = house_info.get('area') or 0
            preco_venda_base = house_info.get('salePrice') or 0
            
            if area <= 40 and (preco_venda_base <= 420000 or preco_venda_base == 0):
                venda_ativa, p_venda, d_venda = checar_mercado(id_imovel, 'comprar')
                alug_ativa, p_aluguel, d_aluguel = checar_mercado(id_imovel, 'alugar')
                
                if venda_ativa and alug_ativa: status_real = 'Venda e Aluguel'
                elif venda_ativa: status_real = 'Apenas Venda'
                elif alug_ativa: status_real = 'Apenas Aluguel'
                else: status_real = 'Indisponível'
                
                old_p_venda = memoria_precos.get(id_imovel, {}).get('venda', 0)
                old_p_aluguel = memoria_precos.get(id_imovel, {}).get('aluguel', 0)
                preco_venda_final = p_venda if p_venda > 0 else old_p_venda
                preco_aluguel_final = p_aluguel if p_aluguel > 0 else old_p_aluguel
                
                cursor.execute("SELECT id_imovel FROM imoveis WHERE id_imovel = ?", (id_imovel,))
                if cursor.fetchone() is None:
                    cursor.execute('''INSERT INTO imoveis (id_imovel, tipo, cidade, bairro, rua, cep, area_m2, quartos, banheiros, vagas, preco_venda, preco_aluguel, condominio, iptu, data_primeira_vista, data_ultima_vista, status, data_pub_venda, data_pub_aluguel) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (id_imovel, house_info.get('type'), house_info.get('city'), house_info.get('region', {}).get('name') or house_info.get('neighborhood'), "Doutor Jubair Celestino", '06216-160', area, house_info.get('bedrooms'), house_info.get('bathrooms'), house_info.get('parkingSpaces'), preco_venda_final, preco_aluguel_final, house_info.get('condoPrice'), house_info.get('iptu'), hoje, hoje, status_real, d_venda, d_aluguel))
                else:
                    cursor.execute('''UPDATE imoveis SET data_ultima_vista = ?, status = ?, preco_venda = ?, preco_aluguel = ?, data_pub_venda = ?, data_pub_aluguel = ? WHERE id_imovel = ?''', (hoje, status_real, preco_venda_final, preco_aluguel_final, d_venda, d_aluguel, id_imovel))
                conn.commit()
    except: pass
    time.sleep(1)

print("\n📸 Verificando imagens pendentes...")
cursor.execute("SELECT id_imovel FROM imoveis")
for (id_imovel,) in cursor.fetchall():
    if not os.path.exists(f"{pasta_fotos}/{id_imovel}_foto_1.jpg"):
        try:
            res = requests.get(f"https://www.quintoandar.com.br/imovel/{id_imovel}", headers=headers)
            if '<script id="__NEXT_DATA__"' in res.text:
                json_str = res.text.split('<script id="__NEXT_DATA__" type="application/json">')[1].split('</script>')[0]
                nomes = list(dict.fromkeys(re.findall(r'([a-zA-Z0-9_.-]*' + str(id_imovel) + r'[a-zA-Z0-9_.-]*\.(?:jpg|jpeg|webp))', json_str) or re.findall(r'(original[a-zA-Z0-9_.-]+\.(?:jpg|jpeg|webp))', json_str)))
                f_salvas = 0
                for nome in nomes:
                    img_data = requests.get(f"https://www.quintoandar.com.br/img/800x600/{nome}", headers=headers).content
                    if len(img_data) > 1000:
                        with open(f"{pasta_fotos}/{id_imovel}_foto_{f_salvas + 1}.jpg", 'wb') as f: f.write(img_data)
                        f_salvas += 1
                        if f_salvas == 3: break
        except: pass

print("🖥️ Construindo Dashboard HTML e Página de Histórico...")
df_imoveis = pd.read_sql_query("SELECT * FROM imoveis", conn)
df_historico = pd.read_sql_query("SELECT * FROM historico_precos", conn)
conn.close()

# ==========================================
# GERAÇÃO DO INDEX.HTML (DASHBOARD PRINCIPAL)
# ==========================================
html = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard Imobiliário</title>
    <style>
        body { margin: 0; padding: 0; background-color: #e9ecef; }
        .dashboard-header { background: #2c3e50; color: white; padding: 20px; font-family: 'Segoe UI', sans-serif; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 15px;}
        .botoes-topo { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
        .botoes-topo a { background-color: #2ecc71; color: white; padding: 8px 15px; border-radius: 6px; text-decoration: none; font-size: 14px; font-weight: bold; box-shadow: 0 2px 4px rgba(0,0,0,0.2); }
        .botoes-topo a.btn-hist { background-color: #f39c12; }
        .control-group { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
        .control-group select { padding: 10px; border-radius: 6px; border: none; font-size: 14px; font-weight: bold; cursor: pointer; }
        .vitrine-container { display: flex; flex-wrap: wrap; gap: 20px; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; padding: 20px; justify-content: center; }
        .card-imovel { background: white; border-radius: 12px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); width: 340px; overflow: hidden; display: flex; flex-direction: column; position: relative; }
        .galeria-fotos { display: flex; overflow-x: auto; scroll-snap-type: x mandatory; }
        .galeria-fotos img { width: 100%; height: 200px; object-fit: cover; scroll-snap-align: center; flex-shrink: 0; }
        .info-imovel { padding: 15px; display: flex; flex-direction: column; gap: 8px; height: 100%; }
        .preco-box { background: #f8f9fa; padding: 12px; border-radius: 8px; margin-top: -5px; border: 1px solid #dee2e6; }
        .linha-mercado { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 10px; }
        .linha-mercado:last-child { margin-bottom: 0; }
        .info-preco { display: flex; flex-direction: column; }
        .preco-venda { font-size: 18px; font-weight: bold; color: #2ecc71; margin: 0; }
        .preco-aluguel { font-size: 16px; font-weight: bold; color: #f39c12; margin: 0; }
        .preco-indisponivel { color: #95a5a6; text-decoration: line-through; font-weight: normal; }
        .data-mercado { font-size: 10px; color: #7f8c8d; margin-top: 2px; }
        .mini-badge { padding: 4px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; color: white; text-transform: uppercase; margin-top: 2px; }
        .badge-on { background-color: #2ecc71; } .badge-off { background-color: #e74c3c; }
        .historico-box { background: #fff3cd; color: #856404; font-size: 11px; padding: 8px; border-radius: 6px; border-left: 4px solid #ffeeba; margin-top: 5px; }
        .custos-extras { font-size: 13px; color: #7f8c8d; margin-top: 5px; }
        .tags { display: flex; gap: 10px; font-size: 13px; font-weight: 500; color: #34495e; background: #ecf0f1; padding: 8px; border-radius: 6px; justify-content: space-around; }
        .endereco { font-size: 13px; color: #95a5a6; margin-bottom: 5px; }
        .datas-info { font-size: 11px; color: #95a5a6; text-align: right; margin-bottom: 5px; }
        .container-botoes { display: flex; gap: 10px; margin-top: auto; }
        .botao-link { text-align: center; color: white; text-decoration: none; padding: 10px; border-radius: 6px; font-weight: bold; transition: 0.2s; font-size: 13px; flex: 1; }
        .botao-venda { background: #2ecc71; } .botao-aluguel { background: #f39c12; } .botao-cinza { background: #95a5a6; }
        .galeria-fotos::-webkit-scrollbar { display: none; }
        .selo-alerta { position: absolute; top: 10px; left: 10px; color: white; padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: bold; z-index: 10; box-shadow: 0 2px 4px rgba(0,0,0,0.3); text-transform: uppercase; }
        .selo-novo { background: #9b59b6; }
        .selo-alterado { background: #e67e22; }
    </style>
</head>
<body>
<div class="dashboard-header">
    <div class="botoes-topo">
        <h2>🏢 Dashboard Imobiliário</h2>
        <a href="https://github.com/GustavoYoshiharuMiyake/dashboard-imoveis/actions" target="_blank">🔄 Forçar Atualização</a>
        <a href="historico.html" class="btn-hist">📖 Ver Histórico Completo</a>
    </div>
    <div class="control-group">
        <select id="filtroStatus" onchange="aplicarFiltros()">
            <option value="Todos">👁️ Todos os Imóveis</option>
            <option value="Apenas Venda">💰 Filtro: Venda Ativa</option>
            <option value="Apenas Aluguel">🔑 Filtro: Aluguel Ativo</option>
        </select>
        <select id="sortOrder" onchange="aplicarFiltros()">
            <option value="default">↕️ Ordenação Padrão</option>
            <option value="vendaAsc">📈 Menor Preço (Venda)</option>
            <option value="vendaDesc">📉 Maior Preço (Venda)</option>
            <option value="aluguelAsc">📈 Menor Preço (Aluguel)</option>
            <option value="aluguelDesc">📉 Maior Preço (Aluguel)</option>
        </select>
    </div>
</div>
<div class='vitrine-container' id="containerCards">
"""

for index, row in df_imoveis.iterrows():
    id_imovel = row['id_imovel']
    status = row.get('status', 'Desconhecido')
    d_venda = row.get('data_pub_venda') or "Sem registro"
    d_aluguel = row.get('data_pub_aluguel') or "Sem registro"
    val_venda = row.get('preco_venda') or 0
    val_aluguel = row.get('preco_aluguel') or 0

    atributos_data = f"data-status='{status}' data-venda='{val_venda}' data-aluguel='{val_aluguel}'"
    
    # === LÓGICA DE SELOS (NOVO / ALTERADO) ===
    selo_html = ""
    is_new = row.get('data_primeira_vista') == hoje
    
    # Verifica se teve alteração HOJE
    mudancas_hoje = df_historico[(df_historico['id_imovel'] == id_imovel) & (df_historico['data_alteracao'] == hoje)]
    is_changed_today = not mudancas_hoje.empty

    if is_new:
        selo_html = "<div class='selo-alerta selo-novo'>✨ Novo Imóvel</div>"
    elif is_changed_today:
        selo_html = "<div class='selo-alerta selo-alterado'>📉 Preço Alterado</div>"

    html_fotos = ""
    for i in range(1, 4):
        caminho_foto = f"fotos_imoveis/{id_imovel}_foto_{i}.jpg"
        if os.path.exists(caminho_foto):
            with open(caminho_foto, "rb") as img_file:
                img_data = img_file.read()
                if b'<!DOCTYPE' not in img_data[:10].upper():
                    img_b64 = base64.b64encode(img_data).decode('utf-8')
                    html_fotos += f"<img src='data:image/*;base64,{img_b64}' alt='Foto {i}'>"
    if not html_fotos:
        html_fotos = "<div style='height: 200px; display:flex; align-items:center; justify-content:center; background:#bdc3c7; color:white;'>Sem fotos</div>"

    historico_html = ""
    mudancas = df_historico[df_historico['id_imovel'] == id_imovel]
    if not mudancas.empty:
        historico_html = "<div class='historico-box'><strong>📉 Histórico Recente:</strong><br>"
        for _, mudanca in mudancas.tail(3).iterrows(): # Mostra apenas as 3 últimas alterações no card
            p_antigo = f"R$ {mudanca['preco_antigo']:,.0f}".replace(',', '.')
            p_novo = f"R$ {mudanca['preco_novo']:,.0f}".replace(',', '.')
            historico_html += f"<span style='display:block; margin-top:2px;'>• {mudanca['data_alteracao']}: {mudanca['mercado']} de {p_antigo} para {p_novo}</span>"
        historico_html += "</div>"
    
    txt_venda = f"R$ {val_venda:,.0f}".replace(',', '.') if val_venda > 0 else "N/A"
    txt_aluguel = f"R$ {val_aluguel:,.0f}".replace(',', '.') if val_aluguel > 0 else "N/A"
    
    if status in ['Apenas Venda', 'Venda e Aluguel']: classe_txt_venda, badge_venda, texto_badge_venda = "preco-venda", "badge-on", "VENDA ON"
    else: classe_txt_venda, badge_venda, texto_badge_venda = "preco-venda preco-indisponivel", "badge-off", "VENDA OFF"
        
    if status in ['Apenas Aluguel', 'Venda e Aluguel']: classe_txt_aluguel, badge_aluguel, texto_badge_aluguel = "preco-aluguel", "badge-on", "ALUG ON"
    else: classe_txt_aluguel, badge_aluguel, texto_badge_aluguel = "preco-aluguel preco-indisponivel", "badge-off", "ALUG OFF"

    condo = f"R$ {row['condominio']:.0f}" if pd.notna(row['condominio']) else "N/A"
    iptu = f"R$ {row['iptu']:.0f}" if pd.notna(row['iptu']) else "N/A"
    
    html_botoes = "<div class='container-botoes'>"
    if status in ['Venda e Aluguel', 'Apenas Venda']: html_botoes += f"<a href='https://www.quintoandar.com.br/imovel/{id_imovel}/comprar' target='_blank' class='botao-link botao-venda'>Ver Venda</a>"
    if status in ['Venda e Aluguel', 'Apenas Aluguel']: html_botoes += f"<a href='https://www.quintoandar.com.br/imovel/{id_imovel}/alugar' target='_blank' class='botao-link botao-aluguel'>Ver Aluguel</a>"
    if status == 'Indisponível': html_botoes += f"<a href='https://www.quintoandar.com.br/imovel/{id_imovel}' target='_blank' class='botao-link botao-cinza'>Anúncio Inativo</a>"
    html_botoes += "</div>"
    
    html += f"""
    <div class='card-imovel' {atributos_data}>
        {selo_html}
        <div class='galeria-fotos'>{html_fotos}</div>
        <div class='info-imovel'>
            <div class='preco-box'>
                <div class='linha-mercado'>
                    <div class='info-preco'><p class='{classe_txt_venda}'>Venda: {txt_venda}</p><span class='data-mercado'>📅 {d_venda}</span></div>
                    <span class='mini-badge {badge_venda}'>{texto_badge_venda}</span>
                </div>
                <div class='linha-mercado'>
                    <div class='info-preco'><p class='{classe_txt_aluguel}'>Alug: {txt_aluguel}</p><span class='data-mercado'>📅 {d_aluguel}</span></div>
                    <span class='mini-badge {badge_aluguel}'>{texto_badge_aluguel}</span>
                </div>
            </div>
            {historico_html}
            <p class='custos-extras'>Condom: {condo} | IPTU: {iptu}</p>
            <div class='tags'><span>📏 {row['area_m2']}m²</span><span>🛏️ {row['quartos']} Qt</span><span>🚗 {row['vagas']} Vg</span></div>
            <p class='endereco'>📍 {row['rua']} - {row['bairro']}</p>
            <div class='datas-info'>Atualizado: {row['data_ultima_vista']}</div>
            {html_botoes}
        </div>
    </div>
    """

html += """
</div>
<script>
function aplicarFiltros() {
    let filtro = document.getElementById('filtroStatus').value;
    let sort = document.getElementById('sortOrder').value;
    let container = document.getElementById('containerCards');
    let cards = Array.from(document.querySelectorAll('.card-imovel'));

    cards.forEach(card => {
        let status = card.getAttribute('data-status');
        let show = false;
        if (filtro === 'Todos') show = true;
        else if (filtro === 'Apenas Venda' && (status === 'Apenas Venda' || status === 'Venda e Aluguel')) show = true;
        else if (filtro === 'Apenas Aluguel' && (status === 'Apenas Aluguel' || status === 'Venda e Aluguel')) show = true;
        card.style.display = show ? 'flex' : 'none';
    });

    cards.sort((a, b) => {
        let vA = parseFloat(a.getAttribute('data-venda')); let vB = parseFloat(b.getAttribute('data-venda'));
        let aA = parseFloat(a.getAttribute('data-aluguel')); let aB = parseFloat(b.getAttribute('data-aluguel'));
        
        let vA_asc = vA === 0 ? Infinity : vA; let vB_asc = vB === 0 ? Infinity : vB;
        let vA_desc = vA === 0 ? -Infinity : vA; let vB_desc = vB === 0 ? -Infinity : vB;
        let aA_asc = aA === 0 ? Infinity : aA; let aB_asc = aB === 0 ? Infinity : aB;
        let aA_desc = aA === 0 ? -Infinity : aA; let aB_desc = aB === 0 ? -Infinity : aB;

        if (sort === 'vendaAsc') return vA_asc - vB_asc;
        if (sort === 'vendaDesc') return vB_desc - vA_desc;
        if (sort === 'aluguelAsc') return aA_asc - aB_asc;
        if (sort === 'aluguelDesc') return aB_desc - aA_desc;
        return 0; 
    });
    cards.forEach(card => container.appendChild(card));
}
</script>
</body>
</html>
"""

with open("index.html", "w", encoding="utf-8") as f:
    f.write(html)

# ==========================================
# GERAÇÃO DO HISTORICO.HTML (PÁGINA SECUNDÁRIA)
# ==========================================
html_hist = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Histórico de Alterações</title>
    <style>
        body { margin: 0; padding: 0; background-color: #e9ecef; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
        .dashboard-header { background: #2c3e50; color: white; padding: 20px; display: flex; justify-content: space-between; align-items: center; }
        .btn-voltar { background-color: #3498db; color: white; padding: 8px 15px; border-radius: 6px; text-decoration: none; font-weight: bold; }
        .content { padding: 30px; max-width: 1000px; margin: 0 auto; }
        table { width: 100%; background: white; border-collapse: collapse; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }
        th { background: #34495e; color: white; padding: 15px; text-align: left; }
        td { padding: 15px; border-bottom: 1px solid #ddd; color: #2c3e50; }
        tr:hover { background-color: #f1f2f6; }
        .p-antigo { color: #e74c3c; text-decoration: line-through; }
        .p-novo { color: #2ecc71; font-weight: bold; }
        .link-id { color: #3498db; text-decoration: none; font-weight: bold; }
    </style>
</head>
<body>
<div class="dashboard-header">
    <h2>📖 Central de Auditoria (Histórico de Preços)</h2>
    <a href="index.html" class="btn-voltar">⬅️ Voltar ao Dashboard</a>
</div>
<div class="content">
"""

if df_historico.empty:
    html_hist += "<div style='background: white; padding: 20px; border-radius: 8px; text-align: center;'><h3>Nenhuma alteração de preço registrada ainda.</h3><p>O robô começou a monitorar os valores base hoje. Volte nos próximos dias!</p></div>"
else:
    html_hist += "<table><tr><th>Data da Alteração</th><th>ID do Imóvel</th><th>Mercado</th><th>Preço Antigo</th><th>Preço Novo</th></tr>"
    # Ordena da alteração mais recente para a mais antiga
    df_hist_sorted = df_historico.sort_values(by='data_alteracao', ascending=False)
    
    for _, h in df_hist_sorted.iterrows():
        p_ant = f"R$ {h['preco_antigo']:,.0f}".replace(',', '.')
        p_nov = f"R$ {h['preco_novo']:,.0f}".replace(',', '.')
        id_im = h['id_imovel']
        html_hist += f"<tr><td>{h['data_alteracao']}</td><td><a href='https://www.quintoandar.com.br/imovel/{id_im}' target='_blank' class='link-id'>#{id_im}</a></td><td>{h['mercado']}</td><td class='p-antigo'>{p_ant}</td><td class='p-novo'>{p_nov}</td></tr>"
    html_hist += "</table>"

html_hist += "</div></body></html>"

with open("historico.html", "w", encoding="utf-8") as f:
    f.write(html_hist)

print("✅ Tudo pronto! Ficheiros index.html e historico.html atualizados.")
