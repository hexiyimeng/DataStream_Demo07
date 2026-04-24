import { type ReactNode, type ButtonHTMLAttributes, forwardRef } from 'react';

export type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'danger' | 'warning';
export type ButtonSize = 'xs' | 'sm' | 'md';

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  active?: boolean;
  loading?: boolean;
  icon?: ReactNode;
  children?: ReactNode;
}

const VARIANT_STYLES: Record<ButtonVariant, string> = {
  primary:
    'bg-[var(--color-accent)] text-white hover:bg-[var(--color-accent-hover)] border border-transparent',
  secondary:
    'bg-[var(--color-bg-field)] text-[var(--color-text-primary)] hover:bg-[var(--color-bg-field-hover)] border border-[var(--color-border-default)]',
  ghost:
    'bg-transparent text-[var(--color-text-secondary)] hover:text-[var(--color-text-primary)] hover:bg-[var(--color-bg-field)] border border-transparent',
  danger:
    'bg-[var(--color-danger-soft)] text-[var(--color-danger)] hover:bg-[var(--color-danger)] hover:text-white border border-[var(--color-danger)]/30',
  warning:
    'bg-[var(--color-warning-soft)] text-[var(--color-warning)] hover:bg-[var(--color-warning)] hover:text-white border border-[var(--color-warning)]/30',
};

const SIZE_STYLES: Record<ButtonSize, string> = {
  xs: 'h-6 px-2 text-[10px] gap-1 rounded-[var(--radius-sm)]',
  sm: 'h-7 px-2.5 text-[11px] gap-1.5 rounded-[var(--radius-md)]',
  md: 'h-8 px-3 text-[12px] gap-2 rounded-[var(--radius-md)]',
};

const DISABLED_STYLE =
  'opacity-50 cursor-not-allowed pointer-events-none';

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      variant = 'secondary',
      size = 'sm',
      active = false,
      loading = false,
      icon,
      children,
      className = '',
      disabled,
      ...props
    },
    ref
  ) => {
    const base = [
      'inline-flex items-center justify-center font-medium transition-all duration-[var(--motion-fast)]',
      'select-none whitespace-nowrap',
      VARIANT_STYLES[variant],
      SIZE_STYLES[size],
      active && !disabled ? 'ring-1 ring-[var(--color-accent)] ring-offset-1 ring-offset-[var(--color-bg-app)]' : '',
      disabled || loading ? DISABLED_STYLE : '',
    ]
      .filter(Boolean)
      .join(' ');

    return (
      <button
        ref={ref}
        className={`${base} ${className}`}
        disabled={disabled || loading}
        {...props}
      >
        {loading ? (
          <svg
            className="w-3 h-3 animate-spin"
            fill="none"
            viewBox="0 0 24 24"
          >
            <circle
              className="opacity-25"
              cx="12"
              cy="12"
              r="10"
              stroke="currentColor"
              strokeWidth="4"
            />
            <path
              className="opacity-75"
              fill="currentColor"
              d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
            />
          </svg>
        ) : icon ? (
          <span className="shrink-0 w-3.5 h-3.5 flex items-center justify-center">
            {icon}
          </span>
        ) : null}
        {children}
      </button>
    );
  }
);
Button.displayName = 'Button';
