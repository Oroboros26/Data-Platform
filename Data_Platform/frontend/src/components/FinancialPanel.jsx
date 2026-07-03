import { useState } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, ReferenceLine
} from 'recharts'
import SankeySvg from './SankeySvg'

const fmt = (v) => {
  if (v == null) return null
  const abs = Math.abs(v)
  if (abs >= 1_000_000) return `${(v / 1_000_000).toFixed(2)} M€`
  if (abs >= 1_000)     return `${(v / 1_000).toFixed(0)} k€`
  return `${v.toFixed(0)} €`
}

const fmtPct = (v) => v != null ? `${v > 0 ? '+' : ''}${v.toFixed(2)} %` : null

const colorClass = (v) => {
  if (v == null) return 'neu'
  return v >= 0 ? 'pos' : 'neg'
}

const fmtAxis = (v) => {
  const abs = Math.abs(v)
  if (abs >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`
  if (abs >= 1_000)     return `${(v / 1_000).toFixed(0)}k`
  return String(v)
}

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null
  return (
    <div style={{
      background: '#0d0d0d', border: '1px solid #2a2a2a', borderRadius: 2,
      padding: '10px 16px', fontSize: '0.75rem',
    }}>
      <div style={{ color: '#6a6a6a', marginBottom: 6, textTransform: 'uppercase', letterSpacing: '0.08em' }}>{label}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ color: '#f0f0f0', fontWeight: 300, marginBottom: 2 }}>
          {p.name} : {fmt(p.value)}
        </div>
      ))}
    </div>
  )
}

function KpiBox({ label, value, unit }) {
  const display = unit === '%' ? fmtPct(value) : fmt(value)
  return (
    <div className="kpi-box">
      <div className="kpi-label">{label}</div>
      {display != null
        ? <div className={`kpi-value ${colorClass(value)}`}>{display}</div>
        : <div className="kpi-null">—</div>
      }
    </div>
  )
}

function RevenueChart({ years }) {
  const chartData = years
    .filter(y => y.ca != null || y.chiffre_affaires_net != null)
    .map(y => ({
      year: String(y.year),
      CA: y.ca ?? y.chiffre_affaires_net,
      RN: y.resultat_net,
    }))
    .sort((a, b) => a.year.localeCompare(b.year))

  if (chartData.length < 2) return null

  return (
    <div className="sankey-wrap">
      <div className="panel-title">Évolution du chiffre d'affaires & résultat net</div>
      <ResponsiveContainer width="100%" height={200}>
        <BarChart data={chartData} barGap={4} margin={{ top: 4, right: 8, left: 8, bottom: 0 }}>
          <XAxis dataKey="year" tick={{ fill: '#6a6a6a', fontSize: 11, letterSpacing: '0.06em' }} axisLine={false} tickLine={false} />
          <YAxis tickFormatter={fmtAxis} tick={{ fill: '#6a6a6a', fontSize: 10 }} axisLine={false} tickLine={false} width={52} />
          <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(255,255,255,0.02)' }} />
          <ReferenceLine y={0} stroke="#1c1c1c" />
          <Bar dataKey="CA" name="Chiffre d'affaires" radius={[2, 2, 0, 0]} maxBarSize={40}>
            {chartData.map((_, i) => <Cell key={i} fill="#e0e0e0" opacity={0.9} />)}
          </Bar>
          <Bar dataKey="RN" name="Résultat net" radius={[2, 2, 0, 0]} maxBarSize={40}>
            {chartData.map((d, i) => (
              <Cell key={i} fill={d.RN >= 0 ? '#ffffff' : '#444444'} opacity={0.9} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      <div style={{ display: 'flex', gap: 24, marginTop: 12, fontSize: '0.6rem', color: '#6a6a6a', justifyContent: 'center', textTransform: 'uppercase', letterSpacing: '0.1em' }}>
        <span style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <span style={{ width: 10, height: 3, background: '#e0e0e0', display: 'inline-block' }} />
          Chiffre d'affaires
        </span>
        <span style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <span style={{ width: 10, height: 3, background: '#ffffff', display: 'inline-block' }} />
          Résultat net
        </span>
      </div>
    </div>
  )
}

export default function FinancialPanel({ financials }) {
  const years = financials?.years || []
  const [selectedYear, setSelectedYear] = useState(years[0]?.year ?? null)

  if (!financials || years.length === 0) {
    return (
      <div className="panel" style={{ marginBottom: 16 }}>
        <div className="panel-title">Données financières</div>
        <p className="no-data">Pas de données financières disponibles pour cette entreprise.</p>
      </div>
    )
  }

  const y = years.find(d => d.year === selectedYear) || years[0]

  return (
    <>
      {/* Evolution chart */}
      <RevenueChart years={years} />

      {/* Sankey */}
      <div className="sankey-wrap">
        <div className="panel-title" style={{ marginBottom: 12 }}>
          Compte de résultats — {y.year}
          {y.entity_name ? ` (${y.entity_name})` : ''}
        </div>
        <div className="year-tabs">
          {years.map(d => (
            <button
              key={d.year}
              className={`year-tab${d.year === y.year ? ' active' : ''}`}
              onClick={() => setSelectedYear(d.year)}
            >{d.year}</button>
          ))}
        </div>
        <SankeySvg yearData={y} />
      </div>

      {/* KPIs */}
      <div className="panel" style={{ marginBottom: 16 }}>
        <div className="panel-title">Indicateurs clés — {y.year}</div>
        <div className="kpi-grid">
          <KpiBox label="Chiffre d'affaires" value={y.ca ?? y.chiffre_affaires_net} />
          <KpiBox label="Marge brute"         value={y.marge_brute} />
          <KpiBox label="EBIT"                value={y.ebit} />
          <KpiBox label="Résultat net"        value={y.resultat_net} />
          <KpiBox label="Trésorerie"          value={y.tresorerie} />
          <KpiBox label="Dettes financières"  value={y.dettes_financieres} />
          <KpiBox label="Fonds propres"       value={y.fonds_propres} />
          <KpiBox label="Capital souscrit"    value={y.capital_souscrit} />
        </div>

        <table className="ratio-table" style={{ marginTop: 4 }}>
          <thead>
            <tr>
              <th>Ratio financier</th>
              <th style={{ textAlign: 'right' }}>Valeur</th>
            </tr>
          </thead>
          <tbody>
            {[
              ['Marge nette',         fmtPct(y.ratios?.marge_nette_pct)],
              ['Marge brute %',       fmtPct(y.ratios?.marge_brute_pct)],
              ['EBITDA %',            fmtPct(y.ratios?.taux_ebitda_pct)],
              ['ROE',                 fmtPct(y.ratios?.roe_pct)],
              ['Liquidité',           y.ratios?.liquidite?.toFixed(3) ?? '—'],
              ["Taux d'endettement",  fmtPct(y.ratios?.taux_endettement_pct)],
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

        <div className="schema-info">
          Schéma : {y.schema_type || '?'} &nbsp;|&nbsp;
          Période : {y.period_start} → {y.period_end} &nbsp;|&nbsp;
          Réf. : {y.reference}
        </div>
      </div>
    </>
  )
}
