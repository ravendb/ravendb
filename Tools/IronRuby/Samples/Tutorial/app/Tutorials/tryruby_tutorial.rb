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

require 'tutorial'

# TryRuby tutorial, adapted from http://tryruby.hobix.com

tutorial 'Try Ruby tutorial' do

  introduction %{
    Got 15 minutes? Give Ruby a shot right now! Ruby is a 
    programming language from Japan (available at ruby-lang.org) 
    which is revolutionizing the web. The beauty of Ruby is found 
    in its balance between simplicity and power.
  }

  section "Chapter 1" do
    
    chapter "Section 1 - The Basics" do

      task :title => 'Using the prompt', 
           :body  => %{
             The grey window below is a Ruby prompt. Type a line of Ruby code, 
             hit Enter  and watch it run!
        
             For example, try typing some math. Like: 
           },
           :code => '2 + 6'

      task :title => 'Numbers & Math',
           :body  => %{
              Good! You did a bit of math. See how the answer popped out?

              Ruby recognizes numbers and mathematic symbols. You could try 
              some other math like:
              
              4 * 10

              5 - 12

              40 / 4

              Sure, computers are handy and fast for math. Let's move on. Want
              to see your name reversed? Type your first name in quotes like 
              this:
           },
           :code => '"Jimmy"'

      task :title => "Say Your Name Backwards",
           :body  => %{
              Perfect, you've formed a string from the letters of your name. A
              string is a set of characters the computer can process.

              Imagine the letters are on a string of laundry line and the 
              quotes are clothespins holding the ends. The quotes mark the 
              beginning and end.

              To reverse your name, type: (Don't forget the dot!)
           },
           :code => '"Jimmy".reverse'

      task :title => 'Counting the Letters',
           :body => %{
              You have used the reverse method on your name! By enclosing your 
              name in quotes, you made a string. Then you called the reverse 
              method, which works on strings to flip all the letters backwards.

              Now, let's see how many letters are in your name:
           },
           :code => '"Jimmy".length'

      task :title => 'On Repeat',
           :body => %{
              Now, I'm sure by now you're wondering what any of this is good 
              for. Well, I'm sure you've been to a website that screamed, 
              Hey, your password is too short! See, some programs use this 
              simple code.

              Watch this. Let's multiply your name by 5.
           }, 
           :code => '"Jimmy" * 5'

      summary :title => 'Hey, Summary #1 Already',
              :body => %{
                 Let's look at what you've learned in the first minute.

                 The prompt. Typing code into the green prompt gives you an 
                 answer from a red prompt. All code gives an answer.
                
                 Numbers and strings are Ruby's math and text objects.
                
                 Methods. You've used English-language methods like reverse and 
                 symbolic methods like * (the multiplication method.) Methods
                 are action!

                 This is the essence of your learning. Taking simple things, 
                 toying with them and turning them into new things. Feeling 
                 comfortable yet? I promise you are.
              }
    end

    chapter "Section 2 - Errors and Arrays" do

      task(:title => 'Do something uncomfortable',
           :body => %{
              Okay, let's do something uncomfortable. Try reversing a number:
           },
           :code => '40.reverse'){ |i| i.error }

      task :title => "Stop, You're Barking Mad!",
           :body => %{
              You can't reverse the number forty. I guess you can hold your 
              monitor up to the mirror, but reversing a number just doesn't 
              make sense. Ruby has tossed an error message. Ruby is telling 
              you there is no method reverse for numbers.

              Maybe if you turn it into a string:
           }, 
           :code => '40.to_s.reverse'

      task :title => 'Boys are Different From Girls',
           :body => %{
              And numbers are different from strings. While you can use methods 
              on any object in Ruby, some methods only work on certain types of 
              things. But you can always convert between different types using 
              Ruby's "to" methods.

              to_s converts things to strings.

              to_i converts things to integers (numbers.)

              to_a converts things to arrays.

              What are arrays?! They are lists. Type in a pair of brackets:
           }, 
           :code => '[]'

      task :title => 'Standing in Line',
           :body => %{
              Great, that's an empty list. Lists store things in order. Like 
              standing in line for popcorn. You are behind someone and you 
              wouldn't dream of pushing them aside, right? And the guy behind
              you, you've got a close eye on him, right?

              Here's a list for you. Lottery numbers:
           },
           :code => '[12, 47, 35]'

      task :title => 'One Raises Its Hand',
           :body => %{
              A list of lottery numbers. Which one is the highest?
              
              Try:
           }, 
           :code => '[12, 47, 35].max'

      task :title => 'Tucking a List Away',
           :body => %{
              Good, good. But it's annoying to have to retype that list, 
              isn't it?

              Let's save our numbers inside a ticket like so
           }, 
           :code => 'ticket = [12, 47, 35]'

      task(:title => 'Now Type Ticket',
           :body => 'Now, type:',
           :code => 'ticket'){|i| i.result == [12, 47, 35]}

      task(:title => 'Saved, Tucked Away',
           :body => %{
              Fantastic! You've hung on to your lotto numbers, tucking them 
              away inside a variable called ticket.

              Let's put your lotto numbers in order, how about? Use:
           },
           :code => 'ticket.sort!'){|i| i.result == [12, 35, 47]}

      summary :title => 'Summary #2 is Upon Us',
              :body => %{
                 You had a list. You sorted the list. The ticket variable is 
                 now changed.

                 Did you notice that the sort! method has a big, bright 
                 exclamation at the end? A lot of times Ruby methods shout 
                 like that if they alter the variable for good. It's nothin 
                 special, just a mark.

                 Now, look how your second minute went:

                 Errors. If you try to reverse a number or do anything fishy,
                 Ruby will skip the prompt and tell you so.

                 Arrays are lists for storing things in order.
                  
                 Variables save a thing and give it a name. You used the equals 
                 sign to do this. Like: ticket = [14, 37, 18].

                 In all there are eight lessons. You are two-eighths of the way 
                 there! This is simple stuff, don't you think? 
                 Good stuff up ahead.
              }

    end

  end

  summary "Thanks for playing!"
end
