#!/usr/bin/awk -f
BEGIN { current = ""; count = 0 }
{
    if ($1 != current) {
	if (current != "") print current, count;
	current = $1;
	count = 0;
    }
    count += $2;
}
END { if (current != "") print current, count; }
