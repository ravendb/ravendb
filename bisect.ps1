$start = $args[0] #bad revision
$end = $args[1] #good revision
$test_prefix = $args[2] #test name (full)

&"git" bisect start $start $end
&"git" bisect run bisect_internal.ps1 $test_prefix
&"git" bisect reset