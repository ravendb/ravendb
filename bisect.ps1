$start = $args[0] #bad revision
$end = $args[1] #good revision
$test_prefix = $args[2] #test name (full)

"Bisecting. Good: $start. Bad: $end. Test: $test_prefix"

&"git" bisect start $start $end
&"git" bisect run bisect_internal.sh $test_prefix
&"git" bisect reset