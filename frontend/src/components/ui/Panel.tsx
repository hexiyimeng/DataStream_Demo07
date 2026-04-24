import { type ReactNode } from 'react';

export type PanelVariant = 'surface' | 'elevated' | 'floating';

interface PanelProps {
  variant?: PanelVariant;
  className?: string;
  children: ReactNode;
  style?: React.CSSProperties;
}

const VARIANT_STYLES: Record<PanelVariant, string> = {
  surface:
    'bg-[var(--color-bg-surface)] border border-[var(--color-border-subtle)]',
  elevated:
    'bg-[var(--color-bg-surface-2)] border border-[var(--color-border-default)] shadow-[var(--shadow-panel)]',
  floating:
    'bg-[var(--color-bg-surface)] border border-[var(--color-border-subtle)] shadow-[var(--shadow-floating)]',
};

export function Panel({
  variant = 'surface',
  className = '',
  children,
  style,
}: PanelProps) {
  return (
    <div
      className={[VARIANT_STYLES[variant], className].filter(Boolean).join(' ')}
      style={style}
    >
      {children}
    </div>
  );
}
