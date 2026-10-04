/** The seal: a custody thread from a person (teal) down to the action, inside an authority ring. */
export function Seal({ size = 26 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" aria-hidden>
      <circle cx="16" cy="16" r="14" fill="none" stroke="var(--ink)" strokeWidth="1.6" />
      <circle cx="16" cy="16" r="11" fill="none" stroke="var(--ink)" strokeWidth="0.8" strokeDasharray="1.2 2.2" opacity="0.6" />
      <path d="M16 8.5 V23.5" stroke="var(--ink)" strokeWidth="1.6" strokeLinecap="round" />
      <circle cx="16" cy="8.5" r="2.8" fill="var(--verified)" />
      <circle cx="16" cy="16" r="2" fill="var(--ink)" />
      <circle cx="16" cy="23.5" r="2" fill="var(--ink)" />
    </svg>
  )
}
