require 'pathname'
require 'date'
#require 'pry'

# parse zzze00301.dat files
# developed and tested with jruby-9.1.2.0
#
# invocation:
#
# /usr/bin/time env JRUBY_OPTS=-J-Xmx2048m ruby parse.rb
#
# That's pretty close to the amount of memory actually required. 1536 works too, but seems to slow
# things down, presumably due to garbage collection.

if ARGV.empty?
	# jruby allows 'java.lang.Runtime.getRuntime.availableProcessors' to be called. So that's neat.
	# Maybe I'll use it / require jruby (haven't tried CRuby at all) 
	puts "usage: #{$PROGRAM_NAME} file [number of threads]"
	exit
end

Thread.abort_on_exception = true

# this occurs throughout because I was chasing down a stupid bug, which resulted from omitting 'current'
# in the Thread.current.name= call. I think. Whatever, it works now.
Thread.current.thread_variable_set 'name', Thread.current.name = 'main'

q_in = Queue.new
q_out = Queue.new
dt_in = Queue.new
dt_out = Queue.new
puts_in = Queue.new
# profile the amount of time the main thread blocks on queue reads. It's on average around 2 x 10^-6 seconds.
#q_pop_times = Queue.new

t_puts = Thread.new do
	Thread.current.thread_variable_set 'name', Thread.current.name = 'console writer'
	puts "** t_puts thread running"
	# future consideration: track sequence, prioritize, etc
	while print_line = puts_in.pop
		puts print_line
	end
	puts "** t_puts thread exited"
end

t_chrono = Thread.new do
	Thread.current.thread_variable_set 'name', Thread.current.name = 'chronothread'
	puts_in.push "[ started execution of chronothread ]"

	while _raw_dates_seq = dt_in.pop
		(dt_start_day, dt_start_month, dt_start_year),
			(dt_end_day, dt_end_month, dt_end_year),
			seq = _raw_dates_seq

		dt_out.push [
			((Date.new dt_start_year, dt_start_month, dt_start_day) if dt_start_day),
			((Date.new dt_end_year, dt_end_month, dt_end_day) if dt_end_day),
			seq
		]
	end
	puts_in.push "[ completed execution of chronothread ]"
	dt_out.push Thread.current
end

t_pool = if ARGV[1]; ARGV[1].to_i else 4 end.times.map do |_thread_id| Thread.new _thread_id do |thread_id|
	Thread.current.thread_variable_set 'name', Thread.current.name = (format 'worker #%i', thread_id)
	datetable = (%w{ JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC }.to_enum.with_index 1).to_h
	puts_in.push "[ started execution of thread #{thread_id} ]"

	while _row_seq = q_in.pop
		# this is probably a little faster in two steps
		empid, _, _, _, _, _, dt_start, dt_end, _, rti, dept = ((seq,=_row_seq)[1].force_encoding "ASCII-8BIT").split "\t"

		dt_in.push [
			unless dt_start==""
				dt_start_day, dt_start_month, dt_start_year = dt_start.split '-'

				# these two run at about the same speed. I think String#split may be just slightly faster than
				# chaining two String#partition calls although partition itself is intrinsically faster.

				#dt_start_month, _, dt_start_year = (dt_start_day, = dt_start.partition '-')[-1].partition '-'
				[ dt_start_day.to_i, datetable[dt_start_month], dt_start_year.to_i ]

			end,
			unless dt_end==""
				#dt_end_month, _, dt_end_year = (dt_end_day, = dt_end.partition '-')[-1].partition '-'
				dt_end_day, dt_end_month, dt_end_year = dt_end.split '-'

				[ dt_end_day.to_i, datetable[dt_end_month], dt_end_year.to_i ]
			end,
			seq
		]

		q_out.push [ empid, rti, dept, seq, thread_id ]
	end

	puts_in.push "[ completed execution of thread #{thread_id} ]"

	q_out.push [ thread_id, Thread.current ]

end end

puts_in.push "loading input file... "

# slower than Pathname#each_line, I think. Additionally, we pass off String#force_encoding to worker threads now.

#(((
#	Pathname.new ENV['HOME']	).
#	join 'zzze00301.dat'		).
#	binread.
#	split "\n"			).
#	each_with_index do |row, idx|

row_count=0
(Pathname.new ARGV[0]).each_line "\n" do |row|
	# XXX this appears quite slower than just sending row_count in a new array with row
	#q_in.push row_count.to_s + "\t" + row
	q_in.push [ row_count, row ]
	row_count += 1
end

t_pool.size.times do q_in.push nil end

puts_in.push format "%i rows queued", row_count

puts_in.push format "Running threads:\n\n\t * %s\n\n",
	(Thread.list.map do |t|
		format "\"%s\", %s", t,
			if t.thread_variable? "name"
				format "\'%s\' (same as \'%s\', of course)", t.name, (t.thread_variable_get "name")
			else
				"<anonymous> (really? what about \"%s\"?)" % (t.name='super anonymous'; t.name)
			end
	end.join "\n\t * ")

# test to see if this is faster than just assigning empty array. I kind of doubt it, actually.
# -- wow, it really seems to help! Seconds are shaved off!
seq_to_idx_map=Array.new row_count
#seq_to_idx_map = []

puts_in.push "dequeuing non-date tuples... "

threads_running = t_pool.size

non_date_rows = loop.each_with_object [] do |_,accum|
	# uncomment for a bit of primitive profiling
	#q_pop_start=Process.clock_gettime Process::CLOCK_MONOTONIC
	q_pop=q_out.pop
	#q_pop_times.push (Process.clock_gettime Process::CLOCK_MONOTONIC) - q_pop_start

	if q_pop.size == 2
		puts_in.push format "\t * completion of thread %i (\"%s\") after %i tuples", *q_pop, accum.size
		#t_pool_done << q_pop
		#t_pool.delete q_pop.last
		if threads_running == 1 #unless nil_count < t_pool.size #q_out.empty?
			dt_in.push nil

			puts_in.push "\n\ndone dequeuing #{accum.size} items; sorting... "
			break accum
		end
		threads_running -= 1
	else
		# this can actually happen with a large number of threads, so don't remark on it
		#puts "** t_pool_done is not empty!!" unless t_pool_done.empty?

		# -- maybe faster to track count ourselves rather than calling Array#size on accum?
		seq_to_idx_map[q_pop[-2]] = accum.size
		accum << q_pop
	end
end

puts_in.push "seq_to_idx_map: size %i" % seq_to_idx_map.size

puts_in.push "confirm: q_out is empty? #{q_out.empty?}"

puts_in.push "dequeuing date tuples..."

dt_rowcount, date_rows = loop.with_index.with_object [] do |(_, idx), accum|
	dt_out_pop = dt_out.pop

	if dt_out_pop.is_a? Thread
		puts_in.push format "thread %s named \"%s\" completed", dt_out_pop, dt_out_pop.name
		break [idx, accum]
	end

	ndr_seq_to_idx_map_cached = seq_to_idx_map[dt_out_pop.last]
	accum[ndr_seq_to_idx_map_cached] = non_date_rows[ndr_seq_to_idx_map_cached] + (dt_out_pop.take 2)
end

puts_in.push "#{dt_rowcount} date tuples gathered; data-excluded tuples updated.\n\n"

dt_today = Date.today

puts_in.push "and the top 25 groups by size:\n\n\t" + (
	(date_rows.map do |empid,rti,dept,row_number,thread_id,dt_start,dt_end|
		[rti,dept,empid] unless dt_end && dt_today > dt_end
	end.compact.group_by do |rti,dept,empid|
		"#{rti}-#{dept}"
	end.map do |rti_dept, rti_dept_empids|
		[ rti_dept, rti_dept_empids.size ]
	end.sort do |a,b|
		a.last <=> b.last
	end.last 25).map do |k,v|
		"#{v}:\t#{k}"
	end.reverse.join "\n\t"
)

puts_in.push "\na sample of the parsed raw data collected:\n\n"

puts_in.push ((date_rows.sample 20).map.with_index do |row,idx|
	format "\t%i.\tthread %i, row %i: \t%s;\t%s - %s",
		idx+1,
		row[3],
		row[4],
		(
			row[0..2].map do |part|
				!part.empty? && part || "<none>"
			end.join ", "
		),
		row[-2],
		row[-1]
end.join "\n")

puts_in.push "\nRunning threads:\n\n\t * %s\n\n" % (Thread.list.join "\n\t * ")

puts_in.push "a list of local variables…\n\n\t* %s" % (local_variables.join "\n\t* ")
puts_in.push nil

# XXX should join all created threads just to be safe
t_puts.join

# examine profiling data, review compiled data, etc
#binding.pry
