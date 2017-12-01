unit Conveyers;

interface

uses
  System.SysUtils, System.Classes, System.SyncObjs, System.Generics.Collections;

type

  TLockQueue<T> = class(TObject)
  private
    FQueue: TQueue<T>;
    FMaxQueueSize: integer;
    FPushedTotal, FPoppedTotal: integer;
    function GetCapacity: integer; {$IFNDEF DEBUG} inline; {$ENDIF}
    procedure SetCapacity(const ACapacity: integer); {$IFNDEF DEBUG} inline; {$ENDIF}
    function GetCount: integer; {$IFNDEF DEBUG} inline; {$ENDIF}

  public
    constructor Create(const AMaxQueueSize: integer; const AInitialCapacity: integer = 0);
    destructor Destroy; override;

    function Lock(Timeout: cardinal = INFINITE): boolean; {$IFNDEF DEBUG} inline; {$ENDIF}
    function TryLock: boolean; {$IFNDEF DEBUG} inline; {$ENDIF}
    procedure Unlock; {$IFNDEF DEBUG} inline; {$ENDIF}
    function Wait(Timeout: cardinal = INFINITE): boolean; overload;
      {$IFNDEF DEBUG} inline; {$ENDIF}
    function Wait(const ALock: TObject; Timeout: cardinal = INFINITE): boolean; overload;
      {$IFNDEF DEBUG} inline; {$ENDIF}
    procedure Pulse; {$IFNDEF DEBUG} inline; {$ENDIF}
    procedure PulseAll; {$IFNDEF DEBUG} inline; {$ENDIF}
    function PushItem(const Item: T; Timeout: cardinal = INFINITE;
      APulse: boolean = True): boolean;
      {$IFNDEF DEBUG} inline; {$ENDIF}
    function PopItem(Timeout: cardinal = INFINITE;
      APulse: boolean = True): T; overload;
      {$IFNDEF DEBUG} inline; {$ENDIF}
    function PopItem(var Item: T; Timeout: cardinal = INFINITE;
      APulse: boolean = True): boolean; overload;
      {$IFNDEF DEBUG} inline; {$ENDIF}

    property Capacity: integer read GetCapacity write SetCapacity;
    property Count: integer read GetCount;
    property PushedTotal: integer read FPushedTotal;
    property PoppedTotal: integer read FPoppedTotal;
  end;

type
  TWorkerState =
    (wsCreated, wsReadyToExecute, wsExecute, wsAfterExecute, wsSleep, wsReadyToDestroy);

  TCustomWorker<T> = class abstract
  public
    type
      TWorkerNotifyEvent = procedure (const Worker: TCustomWorker<T>;
        var KeepWorker: boolean) of object;
      TWorkerExceptionEvent = procedure (const Worker: TCustomWorker<T>;
        const E: Exception; var KeepWorker: boolean) of object;
  private
    FState: TWorkerState;
    FNotify: TWorkerNotifyEvent;
    FHandleException: TWorkerExceptionEvent;
    FRunCount: integer;

    procedure SetState(AState: TWorkerState); {$IFNDEF DEBUG} inline; {$ENDIF}
  protected
    procedure DoExecute(const Data: T);
    procedure Execute(const Data: T); virtual; abstract;
    function Lock(Timeout: cardinal = INFINITE): boolean;
    {$IFNDEF DEBUG} inline; {$ENDIF}
    procedure Unlock; {$IFNDEF DEBUG} inline; {$ENDIF}
    function Wait(Timeout: cardinal = INFINITE): boolean;
    {$IFNDEF DEBUG} inline; {$ENDIF}

    property Notify: TWorkerNotifyEvent
      read FNotify write FNotify;
    property HanldeException: TWorkerExceptionEvent
      read FHandleException write FHandleException;
  public
    procedure Pulse;{$IFNDEF DEBUG} inline; {$ENDIF}
    procedure PulseAll;{$IFNDEF DEBUG} inline; {$ENDIF}
    procedure DoNotify(var KeepWorker: boolean); virtual;
    procedure DoHandleException(const E: Exception;
      var KeepWorker: boolean); virtual;

    property State: TWorkerState read FState;
    property RunCount: integer read FRunCount;
end;

 TCustomConveyer<T; W: TCustomWorker<T>, constructor> = class(TThread)
 private
   type
     TWorker = TCustomWorker<T>;
     TWorkerNotifyEvent = TCustomWorker<T>.TWorkerNotifyEvent;
     TWorkerExceptionEvent = TCustomWorker<T>.TWorkerExceptionEvent;
     TConveyerExceptionEvent = procedure (const Conveyer: TCustomConveyer<T, W>;
        const E: Exception) of object;
     TConveyer = TCustomConveyer<T, W>;
     TWorkerThread = class(TThread)
     private
       FData: T;
       FWorker: TWorker;
       FParent: TConveyer;
       FKeepWorker: boolean;
     protected
       procedure Execute; override;
       procedure DoTerminate; override;
     public
       constructor Create(const AParent: TConveyer; const AData: T;
        const Worker: W; const KeepWorker: boolean);
     end;
  private
    FThreadsEvent: TLightweightEvent;
    FDataQueue: TLockQueue<T>;
    FWorkerQueue: TLockQueue<TWorker>;
    FWorkers: TList<TWorker>;
    FDataQueueLen, FWorkerQueueLen: word;
    FActiveThreadCount: integer;
    FNotify: TWorkerNotifyEvent;
    FHandleWorkerException: TWorker.TWorkerExceptionEvent;
    FHandleConvyerException: TConveyerExceptionEvent;

    procedure SetNotifyEvent(Event: TWorkerNotifyEvent);
      {$IFNDEF DEBUG} inline; {$ENDIF}
    procedure SetHandleWorkerExceptionEvent(Event: TWorkerExceptionEvent);
      {$IFNDEF DEBUG} inline; {$ENDIF}
    procedure SetHandleConveyerExceptionEvent(Event: TConveyerExceptionEvent);
      {$IFNDEF DEBUG} inline; {$ENDIF}
    procedure LockWorkerList; {$IFNDEF DEBUG} inline; {$ENDIF}
    procedure UnLockWorkerList; {$IFNDEF DEBUG} inline; {$ENDIF}
  protected
    procedure ThreadCreated; {$IFNDEF DEBUG} inline; {$ENDIF}
    procedure ThreadTerminated(const Thread: TWorkerThread; KeepWorker: boolean);

    procedure AddWorker(const Worker: TWorker); {$IFNDEF DEBUG} inline; {$ENDIF}
    procedure DeleteWorker(const Worker: TWorker); {$IFNDEF DEBUG} inline; {$ENDIF}
      {$IFNDEF DEBUG} inline; {$ENDIF}
    function GetData(var AData: T): boolean; {$IFNDEF DEBUG} inline; {$ENDIF}
    function GetWorker(var Worker: TWorker; var KeepWorker: boolean): boolean;
    function CreateWorker(var Worker: TWorker; var KeepWorker: boolean): boolean; virtual;
    procedure DestroyWorker(const Worker: TWorker; RemoveFromList: boolean = False);
    procedure DoRunWorker(const AData: T; Worker: W; const KeepWorker: boolean); virtual;

    procedure PulseAllData; {$IFNDEF DEBUG} inline; {$ENDIF}
    procedure PulseAllWorkers; {$IFNDEF DEBUG} inline; {$ENDIF}
    procedure PushWorker(const Worker: TWorker; APulse: boolean = True); overload;
      {$IFNDEF DEBUG} inline; {$ENDIF}
    function PushWorker(const Worker: TWorker; Timeout: cardinal;
      APulse: boolean = True): boolean;
      overload; {$IFNDEF DEBUG} inline; {$ENDIF}
    procedure Execute; override;

    procedure DoHandleConveyerException(const E: Exception); virtual;

    property Notify: TWorkerNotifyEvent write SetNotifyEvent;
    property HandleWorkerException: TWorkerExceptionEvent write SetHandleWorkerExceptionEvent;
    property HandleConvyerException: TConveyerExceptionEvent
      write SetHandleConveyerExceptionEvent;
  public
    constructor Create(ADataQueueLen, AWorkerQueueLen: word;
      CreateSuspended: boolean = False);
    destructor Destroy; override;
    procedure PushData(const AData: T); overload;
    function PushData(const AData: T; Timeout: cardinal): boolean; overload;
    procedure Terminate;
    procedure WaitForTerminate;
end;

implementation

uses
  System.Diagnostics;

{ TLockQueue<T> }

constructor TLockQueue<T>.Create(const AMaxQueueSize, AInitialCapacity: integer);
begin
  inherited Create;
  FQueue:=TQueue<T>.Create;
  FQueue.Capacity:=AInitialCapacity;
  FMaxQueueSize:=AMaxQueueSize;
  FPushedTotal:=0; FPoppedTotal:=0;
end;

destructor TLockQueue<T>.Destroy;
begin
  FQueue.Free;
  inherited;
end;

function TLockQueue<T>.GetCapacity: integer;
begin
  Lock;
  try
    Result:=FQueue.Capacity;
  finally
    Unlock;
  end;
end;

procedure TLockQueue<T>.SetCapacity(const ACapacity: integer);
begin
  Lock;
  try
    FQueue.Capacity:=ACapacity;
  finally
    Unlock;
  end;
end;

function TLockQueue<T>.GetCount: integer;
begin
  Lock;
  try
    Result:=FQueue.Count;
  finally
    Unlock;
  end;
end;

function TLockQueue<T>.Lock(Timeout: cardinal = INFINITE): boolean;
begin
  Result:=TMonitor.Enter(FQueue, Timeout);
end;

function TLockQueue<T>.TryLock: boolean;
begin
  Result:=TMonitor.TryEnter(FQueue);
end;

procedure TLockQueue<T>.Unlock;
begin
  TMonitor.Exit(FQueue);
end;

function TLockQueue<T>.Wait(Timeout: cardinal = INFINITE): boolean;
begin
  Result:=TMonitor.Wait(FQueue, Timeout);
end;

function TLockQueue<T>.Wait(const ALock: TObject; Timeout: cardinal = INFINITE): boolean;
begin
  Result:=TMonitor.Wait(FQueue, ALock, Timeout);
end;

procedure TLockQueue<T>.Pulse;
begin
  TMonitor.Pulse(FQueue);
end;

procedure TLockQueue<T>.PulseAll;
begin
  TMonitor.PulseAll(FQueue);
end;

function IncTimestamp(const X, N: cardinal): cardinal; inline;
begin
  Result:=X;
  if Result <> INFINITE then
    Inc(Result, N);
end;

function DecTimestamp(const X, N: cardinal): cardinal; inline;
begin
  Result:=X;
  if Result <> INFINITE then
    Dec(Result, N);
end;

function TLockQueue<T>.PushItem(const Item: T; Timeout: cardinal = INFINITE;
  APulse: boolean = True): boolean;
var
  TStamp: int64;
  Timer: TStopwatch;

begin
  Result:=False;
  Timer.Create;
  if Timeout < INFINITE then
    Timer.Start;
  TStamp:=Timeout;
  Result:=Lock(Timeout);
  if Result then
  try
    if (FQueue.Count = FMaxQueueSize) then
    begin
      if Timer.IsRunning then
        TStamp:=Timeout-Timer.ElapsedMilliseconds;
      Result:=(FQueue.Count < FMaxQueueSize);
      if not Result and (TStamp > 0) then
        Result:=Wait(TStamp) and (FQueue.Count < FMaxQueueSize);
    end;

    if Result then
    begin
      FQueue.Enqueue(Item);
      TInterlocked.Increment(FPushedTotal);
      if APulse then
        Pulse;
    end;
  finally
    Unlock;
  end;
end;

function TLockQueue<T>.PopItem(var Item: T; Timeout: cardinal = INFINITE;
  APulse: boolean = True): boolean;
var
  TStamp: int64;
  Timer: TStopwatch;

begin
  Item:=Default(T);
  Timer.Create;
  if Timeout < INFINITE then
    Timer.Start;
  TStamp:=Timeout;
  Result:=Lock(Timeout);
  if Result then
  try
    if (FQueue.Count = 0) then
    begin
      if Timer.IsRunning then
        TStamp:=Timeout-Timer.ElapsedMilliseconds;
      Result:=(FQueue.Count > 0);
      if not Result and (TStamp > 0)then
        Result:=Wait(TStamp) and (FQueue.Count > 0);
    end;

    if Result then
    begin
      Item:=FQueue.Dequeue;
      TInterlocked.Increment(FPoppedTotal);
      if APulse then
        Pulse;
    end;
  finally
    Unlock;
  end;
end;

function TLockQueue<T>.PopItem(Timeout: cardinal = INFINITE;
  APulse: boolean = True): T;
begin
  PopItem(Result, TimeOut, APulse);
end;

{ TCustomWorker<T> }

procedure TCustomWorker<T>.SetState(AState: TWorkerState);
begin
  FState:=AState;
end;

procedure TCustomWorker<T>.DoExecute(const Data: T);
begin
  Lock;
  try
    Inc(FRunCount);
    Execute(Data);
  finally
    Unlock;
  end;
end;

function TCustomWorker<T>.Lock(Timeout: cardinal = INFINITE): boolean;
begin
  Result:=TMonitor.Enter(Self, Timeout);
end;

procedure TCustomWorker<T>.Unlock;
begin
  TMonitor.Exit(Self);
end;

function TCustomWorker<T>.Wait(Timeout: cardinal = INFINITE): boolean;
begin
  Result:=TMonitor.Wait(Self, Timeout);
end;

procedure TCustomWorker<T>.Pulse;
begin
  TMonitor.Pulse(Self);
end;

procedure TCustomWorker<T>.PulseAll;
begin
  TMonitor.PulseAll(Self);
end;

procedure TCustomWorker<T>.DoNotify(var KeepWorker: boolean);
begin
  if Assigned(FNotify) then
    FNotify(Self, KeepWorker);
end;

procedure TCustomWorker<T>.DoHandleException(const E: Exception;
      var KeepWorker: boolean);
begin
  if Assigned(FHandleException) then
    FHandleException(Self, E, KeepWorker);
end;

{ TCustomConveyer<T, W>.TWorkerThread }

constructor TCustomConveyer<T, W>.TWorkerThread.Create(const AParent: TConveyer;
  const AData: T; const Worker: W; const KeepWorker: boolean);
begin
  FKeepWorker:=KeepWorker;
  FParent:=AParent;
  FData:=AData;
  FWorker:=Worker;
  FreeOnTerminate:=True;
  if Assigned(AParent) then
    AParent.ThreadCreated;
  inherited Create(False);
end;

procedure TCustomConveyer<T, W>.TWorkerThread.Execute;
begin
  FKeepWorker:=True;
  if Assigned(FWorker) then
  with FWorker do
  try
    SetState(wsReadyToExecute);
    DoNotify(FKeepWorker);
    SetState(wsExecute);
    DoExecute(FData);
    SetState(wsAfterExecute);
    DoNotify(FKeepWorker);
    SetState(wsSleep);
  except
    on E: Exception do
    begin
      DoHandleException(E, FKeepWorker);
    end;
  end;
end;

procedure TCustomConveyer<T, W>.TWorkerThread.DoTerminate;
begin
  if Assigned(OnTerminate) then
    OnTerminate(Self);
  if Assigned(FParent) then
    FParent.ThreadTerminated(Self, FKeepWorker);
end;

{ TCustomConveyer<T, W> }

constructor TCustomConveyer<T, W>.Create(ADataQueueLen, AWorkerQueueLen: word;
  CreateSuspended: boolean = False);
begin
  FDataQueueLen:=ADataQueueLen;
  FWorkerQueueLen:=AWorkerQueueLen;
  FDataQueue:=TLockQueue<T>.Create(ADataQueueLen);
  FWorkerQueue:=TLockQueue<TWorker>.Create(AWorkerQueueLen);
  FWorkers:=TList<TWorker>.Create;
  FThreadsEvent:=TLightweightEvent.Create;
  inherited Create(CreateSuspended);
end;

destructor TCustomConveyer<T, W>.Destroy;
var
  Worker: TWorker;

begin
  LockWorkerList;
  try
    for Worker in FWorkers do
    with Worker do
      DestroyWorker(Worker, False);
  finally
    UnLockWorkerList;
  end;
  FThreadsEvent.Free;
  FWorkerQueue.Free;
  FDataQueue.Free;
  inherited;
end;

procedure TCustomConveyer<T, W>.LockWorkerList;
begin
  TMonitor.Enter(FWorkers);
end;

procedure TCustomConveyer<T, W>.UnLockWorkerList;
begin
  TMonitor.Exit(FWorkers);
end;

procedure TCustomConveyer<T, W>.ThreadCreated;
begin
  TInterlocked.Increment(FActiveThreadCount);
end;

procedure TCustomConveyer<T, W>.ThreadTerminated(const Thread: TWorkerThread;
  KeepWorker: boolean);
begin
  if KeepWorker then
  begin
    PushWorker(Thread.FWorker, False);
    TInterlocked.Decrement(FActiveThreadCount);
    PulseAllWorkers;
  end
  else
    begin
      LockWorkerList;
      try
        DestroyWorker(Thread.FWorker, True);
        TInterlocked.Decrement(FActiveThreadCount);
      finally
        UnLockWorkerList;
      end;
    end;
  FThreadsEvent.SetEvent;
end;

procedure TCustomConveyer<T, W>.AddWorker(const Worker: TWorker);
begin
  LockWorkerList;
  try
    FWorkers.Add(Worker);
  finally
    UnLockWorkerList;
  end;
end;

procedure TCustomConveyer<T, W>.DeleteWorker(const Worker: TWorker);
begin
  LockWorkerList;
  try
    FWorkers.Remove(Worker);
  finally
    UnLockWorkerList;
  end;
end;

function TCustomConveyer<T, W>.GetData(var AData: T): boolean;
begin
   Result:=FDataQueue.PopItem(AData);
end;

function TCustomConveyer<T, W>.GetWorker(var Worker: TWorker;
  var KeepWorker: boolean): boolean;

begin
  Result:=False;
  KeepWorker:=True;
  if FWorkerQueue.Lock then
  try
    if FWorkerQueue.Count > 0 then
      Result:=FWorkerQueue.PopItem(Worker);
  finally
    FWorkerQueue.Unlock;
  end;
  LockWorkerList;
  try
    if not Result and (FWorkers.Count < FWorkerQueueLen) then
      Result:=CreateWorker(Worker, KeepWorker);
  finally
    UnLockWorkerList;
  end;
//  if not Result and (FWorkers.Count > 0) then
//    Result:=FWorkerQueue.PopItem(Worker);
end;

function TCustomConveyer<T, W>.CreateWorker(var Worker: TWorker;
  var KeepWorker: boolean): boolean;
begin
  Result:=False;
  KeepWorker:=True;
  try
    Worker:=W.Create;
    try
      Worker.FNotify:=FNotify;
      Worker.FHandleException:=FHandleWorkerException;
      Worker.SetState(wsCreated);
      Worker.DoNotify(KeepWorker);
      AddWorker(Worker);
      Result:=True;
    except
      on E: Exception do
      begin
        Worker.DoHandleException(E, KeepWorker);
        if not KeepWorker then
        begin
          DestroyWorker(Worker, Result);
          Result:=False;
          Worker:=nil;
          Raise;
        end;
      end;
    end;
  except
    on E: Exception do
      DoHandleConveyerException(E);
  end;
end;

procedure TCustomConveyer<T, W>.PulseAllData;
begin
  FDataQueue.PulseAll;
end;

procedure TCustomConveyer<T, W>.PulseAllWorkers;
begin
  FWorkerQueue.PulseAll;
end;

procedure TCustomConveyer<T, W>.PushWorker(const Worker: TWorker; APulse: boolean);
begin
  FWorkerQueue.PushItem(Worker);
end;

function TCustomConveyer<T, W>.PushWorker(const Worker: TWorker;
  Timeout: cardinal; APulse: boolean): boolean;
begin
  Result:=FWorkerQueue.PushItem(Worker, Timeout, APulse);
end;

procedure TCustomConveyer<T, W>.Execute;
var
  Data: T;
  Worker: TWorker;
  D, W, KeepWorker: boolean;

begin
  FThreadsEvent.SetEvent;
  D:=False; W:=False;
  while True do
  begin
    if (not D) and Terminated and (FDataQueue.Count = 0) then
      Break;
    if not D then
      D:=GetData(Data);
    if D and not W then
      W:=GetWorker(Worker, KeepWorker);
    if D and W then
    begin
      DoRunWorker(Data, Worker, KeepWorker);
      D:=False; W:=False;
    end;
  end;

  while FActiveThreadCount > 0 do
  begin
    if FThreadsEvent.WaitFor = wrSignaled then
      FThreadsEvent.ResetEvent;
  end;
end;

procedure TCustomConveyer<T, W>.DestroyWorker(const Worker: TWorker;
  RemoveFromList: boolean = False);
var
  KeepWorker: boolean;

begin
  if Assigned(Worker) then
  begin
    KeepWorker:=False;
    try
      if RemoveFromList then
        DeleteWorker(Worker);
      Worker.FState:=wsReadyToDestroy;
      try
        Worker.DoNotify(KeepWorker);
      except
        on E: Exception do
          Worker.DoHandleException(E, KeepWorker);
      end;
    finally
      Worker.Free;
    end;
  end;
end;

procedure TCustomConveyer<T, W>.DoHandleConveyerException(const E: Exception);
begin
  if Assigned(FHandleConvyerException) then
    FHandleConvyerException(Self, E);
end;

procedure TCustomConveyer<T, W>.DoRunWorker(const AData: T; Worker: W;
  const KeepWorker: boolean);
begin
  if FActiveThreadCount < FWorkerQueueLen then
    TWorkerThread.Create(Self, AData, Worker, KeepWorker)
  else
    FThreadsEvent.WaitFor;
end;

procedure TCustomConveyer<T, W>.PushData(const AData: T);
begin
  while not FDataQueue.PushItem(AData, INFINITE) do;
end;

function TCustomConveyer<T, W>.PushData(const AData: T; Timeout: cardinal): boolean;
begin
  Result:=FDataQueue.PushItem(AData, Timeout);
end;

procedure TCustomConveyer<T, W>.Terminate;
begin
  inherited;
  FDataQueue.PulseAll;
  FWorkerQueue.PulseAll;
end;

procedure TCustomConveyer<T, W>.WaitForTerminate;
begin
  Terminate;
  WaitFor;
end;

procedure TCustomConveyer<T, W>.SetNotifyEvent(Event: TWorkerNotifyEvent);
begin
  if not Started then
    FNotify:=Event;
end;

procedure TCustomConveyer<T, W>.SetHandleWorkerExceptionEvent(Event: TWorkerExceptionEvent);
begin
  if not Started then
    FHandleWorkerException:=Event;
end;

procedure TCustomConveyer<T, W>.SetHandleConveyerExceptionEvent(
  Event: TConveyerExceptionEvent);
begin
  if not Started then
    FHandleConvyerException:=Event;
end;

end.
