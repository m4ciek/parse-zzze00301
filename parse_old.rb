require 'date'
require 'pathname'

datelock = Mutex.new

datetable = [nil,*%w{JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC}].each_with_index.to_h

# XXX use proper benchmark, for pity's sake
dt_bm_start = DateTime.now

q_in = Queue.new
q_out = Queue.new

dt_day, dt_mon, dt_year = Date.today.strftime('%d-%m-%Y').split('-').map &:to_i

# XXX some funky Java call to size tpool appropriately
tpool=5.times.map do |idx| Thread.new do
	dt_today=Date.today
	Thread.current.thread_variable_set "idx", idx
	rowcount=0
	photofinishes=0
	while _row=q_in.pop
		rowcount+=1
		rownum,row=_row.partition("\t").values_at(0,2)
		empid,_start,_end,rti,dept=row.split("\t").values_at(0,6,7,9,10)

		dt_st, dt_end = [_start, _end].map do |raw_date| [
			raw_date, *unless raw_date.empty?

				_dt_st_d,dt_st_m,_dt_st_y=raw_date.split '-'
				dt_st_y=_dt_st_y.to_i

				if (year_cmp=(dt_st_y <=> dt_year)) < 0
					true
				elsif year_cmp > 0
					false
				elsif (month_cmp=(datetable.fetch(dt_st_m) <=> dt_mon)) < 0
					true
				elsif month_cmp > 0
					false
				else
					# too close for our dumb algorithm to call
					photofinishes+=1
					datelock.synchronize do
						dt_parsed=Date.strptime raw_date, '%d-%b-%Y'
						[ dt_parsed < dt_today, dt_parsed ]
					end
				end
			end
		] end

		q_out.push([dt_st.first, dt_end.first, empid, rti, dept, rownum]) if (dt_end.one? || !dt_end[1])
	end
	Thread.current.thread_variable_set "rowcount", rowcount
	Thread.current.thread_variable_set "photofinishes", photofinishes
end end

#Pathname.new(ENV['HOME']).join('zzze00301.dat').read.force_encoding("ASCII-8BIT").split("\n").sample(15000).each do |row|
rowcount=-1*tpool.size
(Pathname.new(ENV['HOME']).join('zzze00301.dat').read.force_encoding("ASCII-8BIT").split("\n")+Array.new(tpool.size)).each do |row|
	rowcount+=1
	q_in.push(("#{rowcount+7}\t#{row}" if row))
end

puts "#{rowcount} rows pushed"

tpool.each do |t|
	print "wait... "
	t.join
	puts format "thread #%i (\"%s\") completed with %i items processed; %i date lock acquisitions",t.thread_variable_get("idx"),t,t.thread_variable_get("rowcount"),t.thread_variable_get("photofinishes")
end

rowcount=0
res=loop.each_with_object([]) do |_,accum|
	break accum if q_out.empty?
	rowcount+=1
	accum << q_out.pop
end.group_by do |dt_st,dt_end,empid,rti,dept,rownum|
	#puts "row: #{rownum}"
	"#{rti}-#{dept}"
end.map do |group_name,junk|
	[group_name,junk.size]
end

puts "#{rowcount} q_out items removed"

p([
	(DateTime.now-dt_bm_start).to_f*3600*24,
	(res.sort do |a,b| a.last<=>b.last end.last 20)
])
