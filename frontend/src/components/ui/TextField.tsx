import { type InputHTMLAttributes, type ReactNode, forwardRef } from 'react';

interface TextFieldProps extends Omit<InputHTMLAttributes<HTMLInputElement>, 'size'> {
  size?: 'sm' | 'md';
  icon?: ReactNode;
  iconPosition?: 'left' | 'right';
  onClear?: () => void;
}

const SIZE_STYLES = {
  sm: 'h-7 text-[11px] pl-7 pr-7 py-1',
  md: 'h-8 text-[12px] pl-8 pr-8 py-1.5',
};

export const TextField = forwardRef<HTMLInputElement, TextFieldProps>(
  (
    {
      size = 'sm',
      icon,
      iconPosition = 'left',
      onClear,
      className = '',
      value,
      ...props
    },
    ref
  ) => {
    const hasValue = value !== undefined && value !== '';
    return (
      <div className="relative w-full">
        {icon && iconPosition === 'left' && (
          <span className="absolute left-2.5 top-1/2 -translate-y-1/2 text-[var(--color-text-muted)] w-3.5 h-3.5 flex items-center justify-center pointer-events-none">
            {icon}
          </span>
        )}
        <input
          ref={ref}
          value={value}
          className={[
            'w-full rounded-[var(--radius-md)] border border-[var(--color-border-default)]',
            'bg-[var(--color-bg-field)] text-[var(--color-text-primary)]',
            'placeholder-[var(--color-text-muted)]',
            'outline-none transition-colors duration-[var(--motion-fast)]',
            'focus:border-[var(--color-border-focus)] focus:ring-1 focus:ring-[var(--color-border-focus)]/30',
            'disabled:opacity-50 disabled:cursor-not-allowed',
            SIZE_STYLES[size],
            icon && iconPosition === 'left' ? 'pl-8' : '',
            (icon && iconPosition === 'right') || onClear ? 'pr-8' : '',
            className,
          ]
            .filter(Boolean)
            .join(' ')}
          {...props}
        />
        {onClear && hasValue && (
          <button
            onClick={onClear}
            className="absolute right-2 top-1/2 -translate-y-1/2 text-[var(--color-text-muted)] hover:text-[var(--color-text-primary)] transition-colors w-3.5 h-3.5 flex items-center justify-center"
          >
            <svg fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2} className="w-3 h-3">
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        )}
        {!icon && iconPosition === 'right' && (
          <span className="absolute right-2.5 top-1/2 -translate-y-1/2 text-[var(--color-text-muted)] w-3.5 h-3.5 flex items-center justify-center pointer-events-none">
            {icon}
          </span>
        )}
      </div>
    );
  }
);
TextField.displayName = 'TextField';
