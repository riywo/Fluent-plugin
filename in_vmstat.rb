module Fluent


class VmstatInput < Input
  Plugin.register_input('vmstat', self)

  def initialize
    @parser = TextParser.new
  end

  def configure(conf)
    if command = conf['command']
      @vmstat = command
    else
      @vmstat = 'vmstat -n'
    end

    result = `#{@vmstat} 1 1`
    columns = result.split("\n")[1].split(nil).map {|column| '(?<'+ column +'>\d+)'}
    conf['format'] = '/\s*' + columns.join('\s+') + '$/'
    @parser.configure(conf)

    if tag = conf['tag']
      @tag = tag
    else
      raise ConfigError, "vmstat: 'tag' parameter is required on stat input"
    end
  end
  def start
    @loop = Coolio::Loop.new
    command = "#{@vmstat} 1"
    $log.debug "following stat of #{command}"
    @loop.attach Handler.new(command, method(:receive_lines))
    @thread = Thread.new(&method(:run))
  end

  def shutdown
    @loop.stop
    @thread.join
  end

  def run
    @loop.run
  rescue
    $log.error "unexpected error", :error=>$!.to_s
    $log.error_backtrace
  end

  def receive_lines(lines)
    array = []
    lines.each {|line|
      begin
        line.rstrip!  # remove \n
        time, record = @parser.parse(line)
        if time && record
          array << Event.new(time, record)
        end
      rescue
        $log.warn line.dump, :error=>$!.to_s
        $log.debug_backtrace
      end
    }

    unless array.empty?
      Engine.emit_stream(@tag, ArrayEventStream.new(array))
    end
  end

  # seek to the end of file first.
  # logs never duplicate but may be lost if fluent is down.
  class Handler < Coolio::IOWatcher
    def initialize(command, callback)
#      @pos = File.stat(path).size
#      @buffer = ''
      @callback = callback
      @open_proc = open("| #{command}")
      super(@open_proc)
    end

    def on_readable
      lines = []

#      File.open(path) {|f|
#        if f.lstat.size < @pos
#          # moved or deleted
#          @pos = 0
#        else
#          f.seek(@pos)
#        end
        line = @open_proc.gets
        unless line
          return
        end

#        @buffer << line
#        unless line[line.length-1] == ?\n
#          @pos = f.pos
#          return
#        end

#        lines << @buffer
#        @buffer = ''

#        while line = @open_proc.gets
#          unless line[line.length-1] == ?\n
#            @buffer = line
#            break
#          end
#          lines << line
#        end
#        @pos = f.pos
#      }
      lines << line
      @callback.call(lines)

    rescue Errno::ENOENT
      # moved or deleted
#      @pos = 0
    end
  end
end

__END__

<source>
  type vmstat
  tag debug.vmstat
</source>

2011-09-30 11:56:25 +0900 debug.vmstat: {"r":"0","b":"0","swpd":"0","free":"158644","buff":"196544","cache":"6697400","si":"0","so":"0","bi":"0","bo":"5","in":"0","cs":"0","us":"2","sy":"0","id":"97","wa":"0","st":"0"}
