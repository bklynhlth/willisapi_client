# website:   https://www.brooklyn.health


class WillisException(Exception):
    """A Willis call failed: the API (or the client's own checks) returned a 4xx/5xx.

    Raised by ``coa_upload`` and ``refresh_api_key`` (unless ``raise_error=False``).

    The same fields as the failure summary the API returns:

    ``status``   ``"FAILED"``
    ``code``     the HTTP-style code as a string, e.g. ``"400"``
    ``message``  the standard (or the API's) message for it
    ``records``  the per-record results, each ``{record_id, StudyOID, SubjectKey,
                 StudyEventOID, FormOID, status, code, message, errors}``; ``[]``
                 when the failure is not about particular records

    ``errors`` holds top-level problems (e.g. every local validation error) and
    ``summary`` the whole body, including ``session_id`` when the API sent one.
    """

    def __init__(self, code, message: str, errors=None, summary=None):
        self.summary = summary or {}
        self.status = self.summary.get("status", "FAILED")
        self.code = str(code)
        self.message = message
        self.records = self.summary.get("records") or []
        self.errors = errors or []
        lines = [f"{self.code}: {message}"]
        if self.errors:
            lines.append(f"Errors: {self.errors}")
        for position, record in enumerate(self.records):
            if not isinstance(record, dict) or not record.get("errors"):
                continue
            who = f" ({record['record_id']})" if record.get("record_id") else ""
            problems = record["errors"]
            problems = [problems] if not isinstance(problems, list) else problems
            lines.append(
                f"Record {position}{who} {record.get('status', 'FAILED')}: "
                + "; ".join(str(p) for p in problems)
            )
        super().__init__("\n".join(lines))
