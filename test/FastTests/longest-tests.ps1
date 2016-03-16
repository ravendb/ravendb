function Take-Count() {
    param ( [int]$count = $(throw "Need a count") )
    begin { 
        $total = 0;
    }
    process { 
        if ( $total -lt $count ) {
            $_
        }
        $total += 1
    }
}



dnx test  -xml test-timings.xml

[xml]$tests = Get-Content test-timings.xml
$tests.assemblies.assembly.collection.test | 
	sort @{e={$_.time -as [double]} } -descending | 
	% {$_.time + "  " + $_.name } | 
	take-count 15

