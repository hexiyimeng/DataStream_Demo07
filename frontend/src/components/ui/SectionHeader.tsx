import { type ReactNode } from 'react';

interface SectionHeaderProps {
  label: string;
  count?: number;
  action?: ReactNode;
  className?: string;
}

export function SectionHeader({
  label,
  count,
  action,
  className = '',
}: SectionHeaderProps) {
  return (
    <div
      className={[
        'flex items-center justify-between px-2 py-1',
        'border-b border-[var(--color-border-subtle)]',
        className,
      ]
        .filter(Boolean)
        .join(' ')}
    >
      <div className="flex items-center gap-2">
        <span className="text-[10px] font-bold uppercase tracking-wider text-[var(--color-text-muted)]">
          {label}
        </span>
        {count !== undefined && (
          <span className="text-[9px] text-[var(--color-text-muted)] bg-[var(--color-bg-field)] px-1 py-0.5 rounded-full">
            {count}
          </span>
        )}
      </div>
      {action}
    </div>
  );
}
