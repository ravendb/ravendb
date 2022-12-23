# from https://www.alexgallego.org/perf/compiler/explorer/flatbuffers/smf/2018/06/30/effects-cpu-turbo.html

function cpu_disable_performance_cpupower_state(){
    for c in  /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_governor; do echo powersave > $c; done
}
function cpu_enable_performance_cpupower_state(){
    for c in  /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_governor; do echo performance > $c; done
}
function cpu_available_frequencies() {
    local cpuspec=${1:-[0-9]}
    
    for i in /sys/devices/system/cpu/cpu$cpuspec*; do
        echo "$i:"
        echo "    cpufreq/scaling_min_freq: $(cat $i/cpufreq/scaling_min_freq)";
        echo "    cpufreq/scaling_max_freq: $(cat $i/cpufreq/scaling_max_freq)";
    done
}

function cpu_set_min_frequencies() {
    local freq=$1;
    local cpuspec=${2:-[0-9]}
    if [[ $freq == "" ]]; then exit 1; fi
    for i in /sys/devices/system/cpu/cpu$cpuspec*; do
        echo "$i:"
        echo "$i/cpufreq/scaling_min_freq: $(cat $i/cpufreq/scaling_min_freq)";
        echo "$freq" | sudo tee "$i/cpufreq/scaling_min_freq"
        echo "$i/cpufreq/scaling_min_freq: $(cat $i/cpufreq/scaling_min_freq)";
    done
}

function cpu_set_max_frequencies() {
    local freq=$1;
    local cpuspec=${2:-[0-9]}
    if [[ $freq == "" ]]; then exit 1; fi
    for i in /sys/devices/system/cpu/cpu$cpuspec*; do
        echo "$i:"
        echo "$i/cpufreq/scaling_max_freq: $(cat $i/cpufreq/scaling_max_freq)";
        echo "$freq" | sudo tee "$i/cpufreq/scaling_max_freq"
        echo "$i/cpufreq/scaling_max_freq: $(cat $i/cpufreq/scaling_max_freq)";
    done
}
