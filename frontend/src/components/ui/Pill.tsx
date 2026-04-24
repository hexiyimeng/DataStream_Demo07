import { type ReactNode } from 'react';

export type PillVariant =
  | 'idle'
  | 'info'
  | 'running'
  | 'success'
  | 'warning'
  | 'danger'
  | 'muted';

interface PillProps {
  variant?: PillVariant;
  dot?: boolean;
  pulse?: boolean;
  children: ReactNode;
  className?: string;
}

const VARIANT_STYLES: Record<PillVariant, { bg: string; text: string; border: string; dotColor: string }> = {
  idle: {
    bg: 'bg-[var(--color-bg-field)]',
    text: 'text-[var(--color-text-muted)]',
    border: 'border-[var(--color-border-subtle)]',
    dotColor: 'var(--color-text-muted)',
  },
  info: {
    bg: 'bg-[var(--color-info-soft)]',
    text: 'text-[var(--color-info)]',
    border: 'border-[var(--color-info)]/30',
    dotColor: 'var(--color-info)',
  },
  running: {
    bg: 'bg-[var(--color-warning-soft)]',
    text: 'text-[var(--color-warning)]',
    border: 'border-[var(--color-warning)]/30',
    dotColor: 'var(--color-warning)',
  },
  success: {
    bg: 'bg-[var(--color-success-soft)]',
    text: 'text-[var(--color-success)]',
    border: 'border-[var(--color-success)]/30',
    dotColor: 'var(--color-success)',
  },
  warning: {
    bg: 'bg-[var(--color-warning-soft)]',
    text: 'text-[var(--color-warning)]',
    border: 'border-[var(--color-warning)]/30',
    dotColor: 'var(--color-warning)',
  },
  danger: {
    bg: 'bg-[var(--color-danger-soft)]',
    text: 'text-[var(--color-danger)]',
    border: 'border-[var(--color-danger)]/30',
    dotColor: 'var(--color-danger)',
  },
  muted: {
    bg: 'bg-transparent',
    text: 'text-[var(--color-text-muted)]',
    border: 'border-[var(--color-border-subtle)]',
    dotColor: 'var(--color-text-muted)',
  },
};

export function Pill({
  variant = 'idle',
  dot = false,
  pulse = false,
  children,
  className = '',
}: PillProps) {
  const s = VARIANT_STYLES[variant];
  return (
    <span
      className={[
        'inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full border text-[10px] font-bold uppercase tracking-wider',
        s.bg,
        s.text,
        s.border,
        className,
      ]
        .filter(Boolean)
        .join(' ')}
    >
      {dot && (
        <span
          className={['w-1.5 h-1.5 rounded-full shrink-0', pulse ? 'animate-pulse' : ''].join(' ')}
          style={{ backgroundColor: s.dotColor }}
        />
      )}
      {children}
    </span>
  );
}
