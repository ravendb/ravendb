# ****************************************************************************
#
# Copyright (c) Microsoft Corporation. 
#
# This source code is subject to terms and conditions of the Apache License, Version 2.0. A 
# copy of the license can be found in the License.html file at the root of this distribution. If 
# you cannot locate the  Apache License, Version 2.0, please send an email to 
# ironruby@microsoft.com. By using this source code in any fashion, you are agreeing to be bound 
# by the terms of the Apache License, Version 2.0.
#
# You must not remove this notice, or any other, from this software.
#
#
# ****************************************************************************

require 'powershell'



game_name = "Age of Empires III Trial"
min_clock = 1400
min_ram = 64000000
game_proc_name = "age3"

puts "This program will determine if your PC meets some of the minimum system requirements to play the game, #{game_name}"

results = $shell.get_process(game_name)

unless results.length.zero?

  mem_usage = results[0].WS/1045576
  proc_id = results[0].ID

  puts "It appears as if you are currently running #{game_name}!"
  puts "The gmae is using #{mem_usage} megs of RAM and has a process ID of #{proc_id}"
  puts
end

video_ram = $shell.get_wmiobject("Win32_VideoController")[0].AdapterRam

unless video_ram
  puts "Cannot determine the amount of RAM on your video card. We'll assume there is enough.'"
  video_ram = min_ram + 1
end

max_clock_speed = $shell.get_wmiobject("Win32_Processor")[0].MaxClockSpeed

has_sound = false
begin
  sound_results = $shell.get_wmiobject("Win32_SoundDevice")[0].Status
  has_sound = true if sound_results.upcase == "OK"
rescue Exception
  has_sound =  false
end

if min_clock > max_clock_speed
  puts "Your system is too slow to play '#{game_name}.'"
  puts "You need a CPU that operates at '#{min_clock/1000}Ghz' or higher"
  puts "Sorry!"
  exit
else 
  puts "Your CPU is fast enough (#{max_clock_speed/1000.0}Ghz)!"
  puts
end

if min_ram > video_ram
  puts "Your video card doesn't have enough memory to play '#{gameName}'."
  puts "You need a video card with at least '#{minVideoRam/1045576}MB'."
  exit
else
  puts "#{video_ram/1045576}MB is enough video memory!"
  puts
end

unless has_sound
  puts "Unfortunately it appears as if you have no sound card."
  puts "Playing #{game_name} would be a much better experience with sound!"
end
    
    
puts "Have a nice day!"
