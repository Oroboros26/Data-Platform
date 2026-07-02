import { useState } from 'react'
import SankeySvg from './SankeySvg'

const fmt = (v) => {
  if (v == null) return null
  const abs = Math.abs(v)
  if (abs >= 1_000_000) return `${(v / 1_000_000).toFixed(2)}M €`
  if (abs >= 1_000) return `${(v / 1_000).toFixed(0)}k €`
  return `${v.toFixed(0)} €`
}

const fmtPct = (v) => v != null ? `${v > 0 ? '+' : ''}${v.toFixed(2)}%` : null

const colorClass = (v) => {
  if (v == null) return 'neu'
  return v >= 0 ? 'pos' : 'neg'
}

function KpiBox({ label, value, unit = '' }) {
  const display = unit === '%' ? fmtPct(value) : fmt(value)
  return (
    <div>
      <div className="kpi-label">{label}</div>
      {display != null
        ? <div className={`kpi-value ${colorClass(value)}`}>{display}</div>
        : <div className="kpi-null">—</div>
      }
    </div>
  )
}

export default function FinancialPanel({ financials }) {
  const years = financials?.years || []
  const [selectedYear, setSelectedYear] = useState(years[0]?.year || null)

  if (!financials || years.length === 0) {
    return (
      <div className="panel" style={{ marginBottom: 20 }}>
        <h3>Données financières</h3>
        <p className="no-data">Pas de données financières disponibles pour cette entreprise.</p>
      </div>
    )
  }

  const y = years.find(d => d.year === selectedYear) || years[0]

  return (
    <>
      {/* Sankey */}
      <div className="sankey-wrap">
        <h3>Compte de résultats — {y.year} {y.entity_name ? `(${y.entity_name})` : ''}</h3>
        <div className="year-tabs">
          {years.map(d => (
            <button
              key={d.year}
              className={`year-tab${d.year === (y?.year) ? ' active' : ''}`}
              onClick={() => setSelectedYear(d.year)}
            >{d.year}</button>
          ))}
        </div>
        <SankeySvg yearData={y} />
      </div>

      {/* KPIs */}
      <div className="panel" style={{ marginBottom: 20 }}>
        <h3>Indicateurs clés — {y.year}</h3>
        <div className="kpi-grid">
          <KpiBox label="Chiffre d'affaires" value={y.ca} />
          <KpiBox label="Marge brute" value={y.marge_brute} />
          <KpiBox label="EBIT" value={y.ebit} />
          <KpiBox label="Résultat net" value={y.resultat_net} />
          <KpiBox label="Trésorerie" value={y.tresorerie} />
          <KpiBox label="Dettes financières" value={y.dettes_financieres} />
          <KpiBox label="Fonds propres" value={y.fonds_propres} />
          <KpiBox label="Capital souscrit" value={y.capital_souscrit} />
        </div>

        <table className="ratio-table" style={{ marginTop: 20 }}>
          <thead>
            <tr>
              <th>Ratio</th>
              <th style={{ textAlign: 'right' }}>Valeur</th>
            </tr>
          </thead>
          <tbody>
            {[
              ['Marge nette', fmtPct(y.ratios?.marge_nette_pct)],
              ['Marge brute %', fmtPct(y.ratios?.marge_brute_pct)],
              ['EBITDA %', fmtPct(y.ratios?.taux_ebitda_pct)],
              ['ROE', fmtPct(y.ratios?.roe_pct)],
              ['Liquidité', y.ratios?.liquidite?.toFixed(3) ?? '—'],
              ["Taux d'endettement", fmtPct(y.ratios?.taux_endettement_pct)],
            ].map(([label, val]) => (
              <tr key={label}>
                <td>{label}</td>
                <td style={{ color: val == null || val === '—' ? '#334155' : '#e2e8f0' }}>
                  {val ?? '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>

        <p style={{ fontSize: '0.72rem', color: '#475569', marginTop: 12 }}>
          Schéma : {y.schema_type || '?'}  |  Période : {y.period_start} → {y.period_end}
          |  Réf. : {y.reference}
        </p>
      </div>
    </>
  )
}
