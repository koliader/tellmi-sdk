package otel

import (
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/trace"
)

// zerologHook forwards every zerolog event to the globally configured OTel
// LoggerProvider and stamps the originating trace/span IDs onto the event so
// console output stays correlated with traces.
type zerologHook struct {
	logger log.Logger
}

// NewZerologHook returns a zerolog.Hook that exports each log record via OTLP.
// Attach it with zerolog.New(...).Hook(otel.NewZerologHook()).
func NewZerologHook() zerolog.Hook {
	return &zerologHook{logger: global.Logger("tellmi")}
}

func (h *zerologHook) Run(e *zerolog.Event, level zerolog.Level, message string) {
	ctx := e.GetCtx()
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		e.Str("trace_id", sc.TraceID().String())
		e.Str("span_id", sc.SpanID().String())
	}

	rec := log.Record{}
	rec.SetTimestamp(time.Now())
	rec.SetSeverity(severity(level))
	rec.SetBody(attribute.StringValue(message))
	if sc.IsValid() {
		rec.AddAttributes(
			attribute.String("trace_id", sc.TraceID().String()),
			attribute.String("span_id", sc.SpanID().String()),
		)
	}
	h.logger.Emit(ctx, rec)
}

// severity maps a zerolog level onto the OTel log severity scale.
func severity(l zerolog.Level) log.Severity {
	switch l {
	case zerolog.TraceLevel:
		return log.SeverityTrace2
	case zerolog.DebugLevel:
		return log.SeverityDebug2
	case zerolog.InfoLevel:
		return log.SeverityInfo2
	case zerolog.WarnLevel:
		return log.SeverityWarn2
	case zerolog.ErrorLevel:
		return log.SeverityError2
	case zerolog.FatalLevel:
		return log.SeverityFatal2
	case zerolog.PanicLevel:
		return log.SeverityFatal4
	case zerolog.NoLevel:
		return log.SeverityInfo1
	default:
		return log.SeverityInfo1
	}
}

var _ zerolog.Hook = (*zerologHook)(nil)
