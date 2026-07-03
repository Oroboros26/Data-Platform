import { useState, useEffect, useCallback } from 'react'
import { searchEnterprises, getEnterprise, getStats } from './api'
import FinancialPanel from './components/FinancialPanel'
import DirigeantsPanel from './components/DirigeantsPanel'
import './index.css'

const FEATURED = [
  { num: '0471530361', name: 'Hotel Exploitatiemaatschappij Diegem', ca: '97,4 M€', city: 'Machelen',  years: 5 },
  { num: '0469456640', name: 'Rocco Forte & Family (Brussels)',       ca: '21,2 M€', city: 'Bruxelles', years: 5 },
  { num: '0465055711', name: 'Renthotel Brussels',                    ca: '20,3 M€', city: 'Bruxelles', years: 5 },
  { num: '0474417201', name: 'Airport Garden Hotel',                  ca: '16,4 M€', city: 'Bruxelles', years: 5 },
  { num: '0402867427', name: 'Hilton International Co (Belgium)',     ca: '13,9 M€', city: 'Antwerpen', years: 4 },
  { num: '0406650328', name: 'Vakantiecentrum La Rose des Sables',   ca: '812 k€',  city: 'Tubize',    years: 8 },
]

// ── Search view ────────────────────────────────────────────────────────────────
function SearchView({ onSelect }) {
  const [q, setQ] = useState('')
  const [results, setResults] = useState([])
  const [loading, setLoading] = useState(false)
  const [stats, setStats] = useState(null)

  useEffect(() => { getStats().then(setStats).catch(() => {}) }, [])

  const handleSearch = useCallback(async (val) => {
    setQ(val)
    if (val.length < 2) { setResults([]); return }
    setLoading(true)
    try { setResults((await searchEnterprises(val)).results || []) }
    catch { setResults([]) }
    finally { setLoading(false) }
  }, [])

  return (
    <>
      <div className="header">
        <div className="logo-row">
          <h1>Data Platform BCE/KBO</h1>
        </div>
        <div className="subtitle">Analyse financière — Secteur hôtelier belge · Architecture Medallion</div>
      </div>

      <div className="stats-row">
        <div className="stat-card">
          <div className="stat-value">{stats ? (stats.scraping?.total ?? '—').toLocaleString('fr-BE') : '…'}</div>
          <div className="stat-label">Hôtels identifiés</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{stats ? (stats.scraping?.done ?? '—').toLocaleString('fr-BE') : '…'}</div>
          <div className="stat-label">Entreprises scrapées</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{stats ? (stats.scraping?.filings ?? '—').toLocaleString('fr-BE') : '…'}</div>
          <div className="stat-label">Dépôts financiers</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{stats ? (stats.gold?.enterprises ?? '—').toLocaleString('fr-BE') : '…'}</div>
          <div className="stat-label">Entreprises Gold</div>
        </div>
      </div>

      <div className="search-section">
        <div className="section-title">Rechercher une entreprise</div>
        <div className="search-input-wrap">
          <input
            className="search-input"
            type="text"
            placeholder="Nom ou numéro BCE (ex: Hilton, Rocco Forte, 0465055711)…"
            value={q}
            onChange={e => handleSearch(e.target.value)}
            autoFocus
          />
        </div>
        {loading && <p style={{ color: '#475569', fontSize: '0.82rem', marginTop: 10 }}>Recherche…</p>}
        {results.length > 0 && (
          <div className="results-list">
            {results.map(r => (
              <div key={r.enterprise_number} className="result-card" onClick={() => onSelect(r.enterprise_number)}>
                <div>
                  <div className="rc-name">{r.name || r.enterprise_number}</div>
                  <div className="rc-meta">
                    {r.enterprise_number}
                    {r.form ? ` · ${r.form}` : ''}
                    {r.city ? ` · ${r.city}` : ''}
                    {r.nace?.length ? ` · NACE ${r.nace[0]}` : ''}
                  </div>
                </div>
                <div className="rc-right">
                  <span className={`rc-badge${r.status === 'Actif' ? '' : ' inactive'}`}>
                    {r.status || '?'}
                  </span>
                </div>
              </div>
            ))}
          </div>
        )}
        {!loading && q.length >= 2 && results.length === 0 && (
          <p style={{ color: '#334155', fontSize: '0.85rem', marginTop: 12 }}>
            Aucun résultat pour « {q} »
          </p>
        )}
      </div>

      {q.length < 2 && (
        <div className="featured-section">
          <div className="section-title">Exemples avec données financières complètes</div>
          <div className="featured-grid">
            {FEATURED.map(f => (
              <div key={f.num} className="featured-card" onClick={() => onSelect(f.num)}>
                <div className="fc-tag">Données complètes · {f.years} ans</div>
                <div className="fc-name">{f.name}</div>
                <div className="fc-ca">{f.ca}</div>
                <div className="fc-meta">{f.city} · {f.num}</div>
              </div>
            ))}
          </div>
        </div>
      )}
    </>
  )
}

// ── Enterprise detail view ─────────────────────────────────────────────────────
function EnterpriseView({ num, onBack }) {
  const [data, setData]       = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError]     = useState(null)

  useEffect(() => {
    setLoading(true); setError(null); setData(null)
    getEnterprise(num)
      .then(setData)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [num])

  if (loading) return <div className="loading">Chargement de la fiche…</div>
  if (error)   return <div className="error-box">Erreur : {error}</div>
  if (!data)   return null

  const { enterprise: e, financials, scrape_status } = data
  const addr = e.address || {}
  const addrStr = [
    addr.StreetFR || addr.StreetNL,
    addr.HouseNumber,
    addr.Zipcode,
    addr.MunicipalityFR || addr.MunicipalityNL,
  ].filter(Boolean).join(' ')

  const mainActs  = (e.activities || []).filter(a => a.Classification === 'MAIN')
  const otherActs = (e.activities || []).filter(a => a.Classification !== 'MAIN')

  return (
    <>
      <button className="back-btn" onClick={onBack}>← Retour</button>

      <div className="ent-header">
        <h2>{e.PrimaryName || e.CommercialName || num}</h2>
        <div className="ent-bce">BCE · {num}</div>
        {addrStr && <div className="ent-address">{addrStr}</div>}
        <div className="ent-meta">
          <span className={`pill${e.Status?.startsWith('AC') ? ' active' : ''}`}>
            {e.StatusLabel || e.Status}
          </span>
          {e.JuridicalFormLabel && <span className="pill">{e.JuridicalFormLabel}</span>}
          {e.StartDate && <span className="pill">Depuis {e.StartDate}</span>}
          {scrape_status?.status === 'done' && (
            <span className="pill scrape">{scrape_status.filings_count} dépôts financiers</span>
          )}
        </div>
      </div>

      <div className="grid-2">
        <div className="panel">
          <div className="panel-title">Activités NACE</div>
          <div className="nace-list">
            {mainActs.map((a, i) => (
              <div key={i} className="nace-item">
                <span className="nace-code">{a.NaceCode}</span>
                <div>
                  <div>{a.NaceLabel || a.NaceCode}</div>
                  <div className="nace-main-badge">Activité principale · {a.NaceVersion}</div>
                </div>
              </div>
            ))}
            {otherActs.slice(0, 3).map((a, i) => (
              <div key={i} className="nace-item">
                <span className="nace-code">{a.NaceCode}</span>
                <div style={{ fontSize: '0.82rem', color: '#94a3b8' }}>{a.NaceLabel || a.NaceCode}</div>
              </div>
            ))}
            {!e.activities?.length && <p className="no-data">Aucune activité enregistrée</p>}
          </div>
        </div>

        <DirigeantsPanel enterpriseNum={num} />
      </div>

      <FinancialPanel financials={financials} />
    </>
  )
}

export default function App() {
  const [selected, setSelected] = useState(null)
  return (
    <div className="app">
      {selected
        ? <EnterpriseView num={selected} onBack={() => setSelected(null)} />
        : <SearchView onSelect={setSelected} />
      }
    </div>
  )
}
